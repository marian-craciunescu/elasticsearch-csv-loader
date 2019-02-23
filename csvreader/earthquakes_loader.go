package csvreader

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/knz/strtime"
	"github.com/marian-craciunescu/elasticsearch-csv-loader/models"
	"github.com/olivere/elastic"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
)

var timeLayout = `%Y/%m/%d %H:%M:%S`

func readEarthQuakeCSV(path string) ([]models.EarthQuake, error) {
	csvFile, err := os.Open(path)
	if err != nil {
		log.WithError(err).Error()
		return nil, err
	}
	var eaarthquakes []models.EarthQuake

	r := csv.NewReader(bufio.NewReader(csvFile))

	i := 0
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		i++
		if i > 1 {
			t, err := strtime.Strptime(record[0], timeLayout)
			if err != nil || t.IsZero() {
				fmt.Print(t)
				log.WithError(err).Error("Could not parse date from file string")
				return nil, err

			}
			lat, err := strconv.ParseFloat(record[1], 64)

			long, err := strconv.ParseFloat(record[2], 64)
			depth, err := strconv.ParseFloat(record[3], 64)
			magnitude, err := strconv.ParseFloat(record[4], 64)
			magType := record[5]
			nbStation, err := strconv.ParseInt(record[6], 10, 32)
			gap := record[7]
			distance, err := strconv.ParseFloat(record[8], 64)
			rms, err := strconv.ParseFloat(record[9], 64)
			src := record[10]
			id := record[11]
			month := t.Month().String()

			e := models.EarthQuake{Time: &t, Latitude: lat, Longitude: long, Depth: depth,
				Magnitude: magnitude, MagType: magType, NbStations: nbStation, Gap: gap,
				Distance: distance,
				RMS:      rms, Source: src, EventID: id, Month: month}

			eaarthquakes = append(eaarthquakes, e)

		}
	}

	return eaarthquakes, nil
}

func SendToElasticSearch() error {
	ctx := context.Background()


	client, err := elastic.NewClient(
		elastic.SetURL("http://localhost:9200"), elastic.SetBasicAuth("elastic","elastic"))
	if err != nil {
		return err
	}

	info, code, err := client.Ping("http://localhost:9200").Do(ctx)
	if err != nil {
		// Handle error
		return err
	}

	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	eaarth, err := readEarthQuakeCSV("../dataset/earthquakes1970-2014.csv")
	if err != nil {
		return nil
	}
	for i, e := range eaarth {
		earthquake, err := json.Marshal(&e)
		if err != nil {
			return err
		}
		resp, err := client.Index().Index("earthquakes").Type("_doc").BodyJson(string(earthquake)).Do(ctx)
		if err != nil {
			fmt.Printf("%d earthquake=%d was nos created. Error : %s \n", i, earthquake, err.Error())
			continue
		}
		fmt.Println(resp)

	}

	return nil
}
