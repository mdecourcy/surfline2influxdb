package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
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
	Spots map[string]string `yaml:"spots"`
}

const maxRetries = 3
const retryDelay = 5 * time.Second

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

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

	token, err := os.ReadFile(filepath.Join(dir, "secrets.txt"))

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

	days, timeInterval := 5, 1

	// Declare a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	errCh := make(chan error, len(cfg.Spots))
	doneCh := make(chan bool, len(cfg.Spots))

	// Iterate over the spots map
	for _, spotID := range cfg.Spots {
		go func(spot string) {
			retries := 0
			for retries < maxRetries {
				if err := fetchAndInsert(spot, days, timeInterval, writeAPI, api); err != nil {
					retries++
					log.Printf("Error on attempt %d for spot %s: %v", retries, spot, err)
					if retries < maxRetries {
						time.Sleep(retryDelay)
						continue
					}
					errCh <- err
				} else {
					doneCh <- true
					break
				}
			}
		}(spotID)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	for i := 0; i < len(cfg.Spots); i++ {
		select {
		case err := <-errCh:
			log.Println("Error:", err)
		case <-doneCh:
			continue
		}
	}
}

func fetchAndInsert(spotId string, days int, timeInterval int, writeAPI api.WriteAPIBlocking, api *surflineapi.SurflineAPI) error {
	if windForecast, err := api.GetWindForecast(spotId, days, timeInterval, true, true); err != nil {
		return fmt.Errorf("error fetching wind forecast for %s: %w", spotId, err)
	} else {
		insertWindForecastToInflux(spotId, windForecast, writeAPI)
	}

	if waveForecast, err := api.GetWaveForecast(spotId, days, timeInterval); err == nil {
		return fmt.Errorf("error fetching wave forecast for %s: %w", spotId, err)
	} else {
		insertWaveForecastToInflux(spotId, waveForecast, writeAPI)
	}

	if tideForecast, err := api.GetTideForecast(spotId, days); err != nil {
		return fmt.Errorf("error fetching tide forecast for %s: %w", spotId, err)
	} else {
		insertTideForecastToInflux(spotId, tideForecast, writeAPI)
	}

	if ratingForecast, err := api.GetSpotForecastRating(spotId, days, timeInterval); err != nil {
		return fmt.Errorf("error fetching spot forecast rating for %s: %w", spotId, err)
	} else {
		insertSpotForecastRatingToInflux(spotId, ratingForecast, writeAPI)
	}

	return nil
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
		forecastTime := time.Unix(waveData.Timestamp, 0)
		currentTime := time.Now().UTC()
		forecastAgeHours := int(currentTime.Sub(forecastTime).Hours())

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

		tagsWithAge := map[string]string{
			"spotId":   spotId,
			"spotName": getFriendlyNameForSpot(spotId),
			"age_h":    fmt.Sprintf("%d", forecastAgeHours),
		}

		tagsWithoutAge := map[string]string{
			"spotId":   spotId,
			"spotName": getFriendlyNameForSpot(spotId),
		}

		// Writing wave forecast data
		pWithAge := influxdb2.NewPoint("waveForecast", tagsWithAge, fields, time.Unix(waveData.Timestamp, 0))
		err := writeAPI.WritePoint(context.Background(), pWithAge)
		if err != nil {
			fmt.Println("Error writing to InfluxDB with age_h tag:", err)
		}

		pWithoutAge := influxdb2.NewPoint("waveForecast", tagsWithoutAge, fields, time.Unix(waveData.Timestamp, 0))
		err = writeAPI.WritePoint(context.Background(), pWithoutAge)
		if err != nil {
			fmt.Println("Error writing to InfluxDB without age_h tag:", err)
		}

		// Writing swells data
		for _, swell := range waveData.Swells {
			swellFields := map[string]interface{}{
				"height":       swell.Height,
				"period":       swell.Period,
				"impact":       swell.Impact,
				"power":        swell.Power,
				"direction":    swell.Direction,
				"directionMin": swell.DirectionMin,
				"optimalScore": swell.OptimalScore,
			}

			pSwellWithAge := influxdb2.NewPoint("swellForecast", tagsWithAge, swellFields, time.Unix(waveData.Timestamp, 0))
			err := writeAPI.WritePoint(context.Background(), pSwellWithAge)
			if err != nil {
				fmt.Println("Error writing swell to InfluxDB with age_h tag:", err)
			}

			pSwellWithoutAge := influxdb2.NewPoint("swellForecast", tagsWithoutAge, swellFields, time.Unix(waveData.Timestamp, 0))
			err = writeAPI.WritePoint(context.Background(), pSwellWithoutAge)
			if err != nil {
				fmt.Println("Error writing swell to InfluxDB without age_h tag:", err)
			}
		}
	}
}

// insertWindForecastToInflux writes wind forecast data to InfluxDB
func insertWindForecastToInflux(spotId string, windForecast *surflineapi.WindForecastResponse, writeAPI api.WriteAPIBlocking) {
	for _, windDetail := range windForecast.Data.Wind {

		forecastTime := time.Unix(windDetail.Timestamp, 0)
		currentTime := time.Now().UTC()
		forecastAgeHours := int(currentTime.Sub(forecastTime).Hours())

		tagsWithAge := map[string]string{
			"location": fmt.Sprintf("%f,%f", windForecast.Associated.Location.Lat, windForecast.Associated.Location.Lon),
			"spotId":   spotId,
			"spotName": getFriendlyNameForSpot(spotId),
			"age_h":    fmt.Sprintf("%d", forecastAgeHours),
		}

		tagsWithoutAge := map[string]string{
			"location": fmt.Sprintf("%f,%f", windForecast.Associated.Location.Lat, windForecast.Associated.Location.Lon),
			"spotId":   spotId,
			"spotName": getFriendlyNameForSpot(spotId),
		}

		fields := map[string]interface{}{
			"speed":         windDetail.Speed,
			"direction":     windDetail.Direction,
			"directionType": windDetail.DirectionType,
			"gust":          windDetail.Gust,
			"optimalScore":  windDetail.OptimalScore,
			"utcOffset":     windDetail.UtcOffset,
		}

		// Writing point with age_h tag
		pWithAge := influxdb2.NewPoint("windForecast", tagsWithAge, fields, time.Unix(windDetail.Timestamp, 0))
		err := writeAPI.WritePoint(context.Background(), pWithAge)
		if err != nil {
			fmt.Println("Error writing to InfluxDB with age_h tag:", err)
		}

		// Writing point without age_h tag
		pWithoutAge := influxdb2.NewPoint("windForecast", tagsWithoutAge, fields, time.Unix(windDetail.Timestamp, 0))
		err = writeAPI.WritePoint(context.Background(), pWithoutAge)
		if err != nil {
			fmt.Println("Error writing to InfluxDB without age_h tag:", err)
		}
	}
}
func insertTideForecastToInflux(spotId string, tideForecast *surflineapi.TideForecastResponse, writeAPI api.WriteAPIBlocking) {
	for _, tideInfo := range tideForecast.Data.Tides {

		forecastTime := time.Unix(tideInfo.Timestamp, 0)
		currentTime := time.Now().UTC()
		forecastAgeHours := int(currentTime.Sub(forecastTime).Hours())

		tagsWithAge := map[string]string{
			"location": fmt.Sprintf("%f,%f", tideForecast.Associated.TideLocation.Lat, tideForecast.Associated.TideLocation.Lon),
			"name":     tideForecast.Associated.TideLocation.Name,
			"spotId":   spotId,
			"spotName": getFriendlyNameForSpot(spotId),
			"age_h":    fmt.Sprintf("%d", forecastAgeHours),
		}

		tagsWithoutAge := map[string]string{
			"location": fmt.Sprintf("%f,%f", tideForecast.Associated.TideLocation.Lat, tideForecast.Associated.TideLocation.Lon),
			"name":     tideForecast.Associated.TideLocation.Name,
			"spotId":   spotId,
			"spotName": getFriendlyNameForSpot(spotId),
		}

		fields := map[string]interface{}{
			"type":      tideInfo.Type,
			"height":    tideInfo.Height,
			"utcOffset": tideInfo.UtcOffset,
		}

		// Writing point with age_h tag
		pWithAge := influxdb2.NewPoint("tideForecast", tagsWithAge, fields, time.Unix(tideInfo.Timestamp, 0))
		err := writeAPI.WritePoint(context.Background(), pWithAge)
		if err != nil {
			fmt.Println("Error writing to InfluxDB with age_h tag:", err)
		}

		// Writing point without age_h tag
		pWithoutAge := influxdb2.NewPoint("tideForecast", tagsWithoutAge, fields, time.Unix(tideInfo.Timestamp, 0))
		err = writeAPI.WritePoint(context.Background(), pWithoutAge)
		if err != nil {
			fmt.Println("Error writing to InfluxDB without age_h tag:", err)
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
		tagsWithAge := map[string]string{
			"spotId":    spotId,
			"ratingKey": rating.Rating.Key,
			"spotName":  getFriendlyNameForSpot(spotId),
			"age_h":     fmt.Sprintf("%d", forecastAgeHours),
		}

		tagsWithoutAge := map[string]string{
			"spotId":    spotId,
			"ratingKey": rating.Rating.Key,
			"spotName":  getFriendlyNameForSpot(spotId),
			"age_h":     fmt.Sprintf("%d", forecastAgeHours),
		}

		// Writing point with age_h tag
		pWithAge := influxdb2.NewPoint("spotForecast", tagsWithAge, fields, time.Unix(rating.Timestamp, 0))
		err := writeAPI.WritePoint(context.Background(), pWithAge)
		if err != nil {
			fmt.Println("Error writing to InfluxDB with age_h tag:", err)
		}

		// Writing point without age_h tag
		pWithoutAge := influxdb2.NewPoint("spotForecast", tagsWithoutAge, fields, time.Unix(rating.Timestamp, 0))
		err = writeAPI.WritePoint(context.Background(), pWithoutAge)
		if err != nil {
			fmt.Println("Error writing to InfluxDB without age_h tag:", err)
		}
	}
}
