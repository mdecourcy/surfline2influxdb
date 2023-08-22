package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	surflineapi "github.com/mdecourcy/go-surfline-api/pkg/surflineapi"
)

const (
	influxDBUrl    = "http://localhost:8086"
	influxDBToken  = ""
	influxDBOrg    = "health"
	influxDBBucket = "surfline"
)

func main() {
	// Setup Surfline API
	client := &http.Client{}
	api := &surflineapi.SurflineAPI{
		HTTPClient: client,
	}

	// Setup InfluxDB client
	influxClient := influxdb2.NewClient(influxDBUrl, influxDBToken)
	defer influxClient.Close()

	writeAPI := influxClient.WriteAPIBlocking(influxDBOrg, influxDBBucket)

	spotId := "5842041f4e65fad6a7708841"
	days, timeInterval := 5, 1

	// Fetch and insert data
	fetchAndInsert(spotId, days, timeInterval, writeAPI, api)
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
		fmt.Println(ratingForecast)
		insertSpotForecastRatingToInflux(spotId, ratingForecast, writeAPI)
	} else {
		fmt.Println("Error fetching spot forecast rating:", err)
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
		tags := map[string]string{
			"spotId": spotId,
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
		tags := map[string]string{
			"location": fmt.Sprintf("%f,%f", windForecast.Associated.Location.Lat, windForecast.Associated.Location.Lon),
			"spotId":   spotId,
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
		tags := map[string]string{
			"location": fmt.Sprintf("%f,%f", tideForecast.Associated.TideLocation.Lat, tideForecast.Associated.TideLocation.Lon),
			"name":     tideForecast.Associated.TideLocation.Name,
			"spotId":   spotId,
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
		fields := map[string]interface{}{
			"ratingValue": rating.Rating.Value,
			"utcOffset":   rating.UtcOffset,
		}
		tags := map[string]string{
			"spotId":    spotId,
			"ratingKey": rating.Rating.Key,
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
