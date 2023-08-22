package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v2"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	surflineapi "github.com/mdecourcy/go-surfline-api/pkg/surflineapi"
)

type Config struct {
	InfluxDB struct {
		Url    string `yaml:"url"`
		Org    string `yaml:"org"`
		Bucket string `yaml:"bucket"`
	} `yaml:"influxdb"`
}

func main() {

	var cfg Config

	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	configFile, err := os.ReadFile(filepath.Join(dir, "config.yaml"))

	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	err = yaml.Unmarshal(configFile, &cfg)
	if err != nil {
		log.Fatalf("Failed to unmarshal config: %v", err)
	}

	token, err := os.ReadFile("secrets.txt")

	if err != nil {
		log.Fatalf("Failed to read secrets file: %v", err)
	}

	influxDBUrl := cfg.InfluxDB.Url
	influxDBToken := string(token)
	influxDBOrg := cfg.InfluxDB.Org
	influxDBBucket := cfg.InfluxDB.Bucket

	// Setup Surfline API
	client := &http.Client{}
	api := &surflineapi.SurflineAPI{
		HTTPClient: client,
	}

	// Setup InfluxDB client
	influxClient := influxdb2.NewClient(influxDBUrl, influxDBToken)
	defer influxClient.Close()

	writeAPI := influxClient.WriteAPIBlocking(influxDBOrg, influxDBBucket)

	pacificBeachSpotId := "5842041f4e65fad6a7708841"
	windanseaSpotId := "5842041f4e65fad6a770883c"
	laJollaShoresSpotId := "5842041f4e65fad6a77088cc"
	oceanBeachSpotId := "5842041f4e65fad6a770883f"

	days, timeInterval := 5, 1

	// Fetch and insert data
	fetchAndInsert(pacificBeachSpotId, days, timeInterval, writeAPI, api)

	fetchAndInsert(windanseaSpotId, days, timeInterval, writeAPI, api)

	fetchAndInsert(laJollaShoresSpotId, days, timeInterval, writeAPI, api)

	fetchAndInsert(oceanBeachSpotId, days, timeInterval, writeAPI, api)
}

func fetchAndInsert(spotId string, days int, timeInterval int, writeAPI api.WriteAPIBlocking, api *surflineapi.SurflineAPI) {
	if windForecast, err := api.GetWindForecast(spotId, days, timeInterval, true, true); err == nil {
		insertWindForecastToInflux(spotId, windForecast, writeAPI)
	} else {
		fmt.Println("Error fetching wind forecast:", err)
	}

	if waveForecast, err := api.GetWaveForecast(spotId, days, timeInterval); err == nil {
		insertWaveForecastToInflux(spotId, waveForecast, writeAPI)
	} else {
		fmt.Println("Error fetching wave forecast:", err)
	}

	if tideForecast, err := api.GetTideForecast(spotId, days); err == nil {
		insertTideForecastToInflux(spotId, tideForecast, writeAPI)
	} else {
		fmt.Println("Error fetching tide forecast:", err)
	}

	if ratingForecast, err := api.GetSpotForecastRating(spotId, days, timeInterval); err == nil {
		insertSpotForecastRatingToInflux(spotId, ratingForecast, writeAPI)
	} else {
		fmt.Println("Error fetching spot forecast rating:", err)
	}
}

// Helper function to get the friendly name
// TODO programatically fetch spot name
func getFriendlyNameForSpot(spotId string) string {
	switch spotId {
	case "5842041f4e65fad6a7708841":
		return "Pacific Beach"
	case "5842041f4e65fad6a770883c":
		return "Windansea Beach"
	case "5842041f4e65fad6a77088cc":
		return "La Jolla Shores"
	case "5842041f4e65fad6a770883f":
		return "Ocean Beach"
	default:
		return "Unknown"
	}
}

func insertWaveForecastToInflux(spotId string, waveForecast *surflineapi.WaveForecastResponse, writeAPI api.WriteAPIBlocking) {
	for _, waveData := range waveForecast.Data.Wave {
		fields := map[string]interface{}{
			"probability":   waveData.Probability,
			"minSurf":       waveData.Surf.Min,
			"maxSurf":       waveData.Surf.Max,
			"optimalScore":  waveData.Surf.OptimalScore,
			"humanRelation": waveData.Surf.HumanRelation,
			"rawMinSurf":    waveData.Surf.Raw.Min,
			"rawMaxSurf":    waveData.Surf.Raw.Max,
			"power":         waveData.Power,
			"utcOffset":     waveData.UtcOffset,
		}
		forecastTime := time.Unix(waveData.Timestamp, 0).UTC()
		currentTime := time.Now().UTC()
		forecastAgeHours := int(currentTime.Sub(forecastTime).Hours())

		tags := map[string]string{
			"spotId":   spotId,
			"spotName": getFriendlyNameForSpot(spotId),
			"age_h":    fmt.Sprintf("%d", forecastAgeHours),
		}

		p := influxdb2.NewPoint("waveForecast",
			tags,
			fields,
			time.Unix(waveData.Timestamp, 0),
		)
		err := writeAPI.WritePoint(context.Background(), p)
		if err != nil {
			fmt.Println("Error writing to InfluxDB:", err)
		}
	}
}

// insertWindForecastToInflux writes wind forecast data to InfluxDB
func insertWindForecastToInflux(spotId string, windForecast *surflineapi.WindForecastResponse, writeAPI api.WriteAPIBlocking) {
	for _, windDetail := range windForecast.Data.Wind {

		forecastTime := time.Unix(windDetail.Timestamp, 0)
		currentTime := time.Now().UTC()
		forecastAgeHours := int(currentTime.Sub(forecastTime).Hours())

		tags := map[string]string{
			"location": fmt.Sprintf("%f,%f", windForecast.Associated.Location.Lat, windForecast.Associated.Location.Lon),
			"spotId":   spotId,
			"spotName": getFriendlyNameForSpot(spotId),
			"age_h":    fmt.Sprintf("%d", forecastAgeHours),
		}

		fields := map[string]interface{}{
			"speed":         windDetail.Speed,
			"direction":     windDetail.Direction,
			"directionType": windDetail.DirectionType,
			"gust":          windDetail.Gust,
			"optimalScore":  windDetail.OptimalScore,
			"utcOffset":     windDetail.UtcOffset,
		}

		p := influxdb2.NewPoint(
			"windForecast",
			tags,
			fields,
			time.Unix(windDetail.Timestamp, 0),
		)

		err := writeAPI.WritePoint(context.Background(), p)
		if err != nil {
			fmt.Println("Error writing wind forecast to InfluxDB:", err)
		}
	}
}
func insertTideForecastToInflux(spotId string, tideForecast *surflineapi.TideForecastResponse, writeAPI api.WriteAPIBlocking) {
	for _, tideInfo := range tideForecast.Data.Tides {

		forecastTime := time.Unix(tideInfo.Timestamp, 0)
		currentTime := time.Now().UTC()
		forecastAgeHours := int(currentTime.Sub(forecastTime).Hours())

		tags := map[string]string{
			"location": fmt.Sprintf("%f,%f", tideForecast.Associated.TideLocation.Lat, tideForecast.Associated.TideLocation.Lon),
			"name":     tideForecast.Associated.TideLocation.Name,
			"spotId":   spotId,
			"spotName": getFriendlyNameForSpot(spotId),
			"age_h":    fmt.Sprintf("%d", forecastAgeHours),
		}

		fields := map[string]interface{}{
			"type":      tideInfo.Type,
			"height":    tideInfo.Height,
			"utcOffset": tideInfo.UtcOffset,
		}

		p := influxdb2.NewPoint(
			"tideForecast",
			tags,
			fields,
			time.Unix(tideInfo.Timestamp, 0),
		)

		err := writeAPI.WritePoint(context.Background(), p)
		if err != nil {
			fmt.Println("Error writing tide forecast to InfluxDB:", err)
		}
	}
}

func insertSpotForecastRatingToInflux(spotId string, ratingForecast *surflineapi.SpotForecastRatingResponse, writeAPI api.WriteAPIBlocking) {
	for _, rating := range ratingForecast.Data.Rating {
		forecastTime := time.Unix(rating.Timestamp, 0).UTC()
		currentTime := time.Now().UTC()
		forecastAgeHours := int(currentTime.Sub(forecastTime).Hours())

		fields := map[string]interface{}{
			"ratingValue": rating.Rating.Value,
			"utcOffset":   rating.UtcOffset,
		}
		tags := map[string]string{
			"spotId":    spotId,
			"ratingKey": rating.Rating.Key,
			"spotName":  getFriendlyNameForSpot(spotId),
			"age_h":     fmt.Sprintf("%d", forecastAgeHours),
		}

		p := influxdb2.NewPoint(
			"spotForecastRating",
			tags,
			fields,
			time.Unix(rating.Timestamp, 0),
		)

		err := writeAPI.WritePoint(context.Background(), p)
		if err != nil {
			fmt.Println("Error writing spot forecast rating to InfluxDB:", err)
		}
	}
}
