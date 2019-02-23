package csvreader

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/marian-craciunescu/elasticsearch-csv-loader/models"
	"github.com/olivere/elastic"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
)

func readMovies(path string) ([]models.Movie, error) {
	csvFile, err := os.Open(path)
	if err != nil {
		log.WithError(err).Error()
		return nil, err
	}
	var movies []models.Movie

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

			duration, err := strconv.ParseInt(record[3], 10, 64)
			if err != nil {
				log.WithError(err).Error("Failed conv duration")
				continue
			}
			year, err := strconv.ParseInt(record[23], 10, 64)
			if err != nil {
				log.WithError(err).Error("Failed conv year ")
				continue
			}
			budget, err := strconv.ParseInt(record[22], 10, 64)
			if err != nil {
				log.WithError(err).Error("Failed conv bugdet")
				continue
			}
			tags := record[9]
			title := record[11]
			directorName := record[1]
			desc := record[16]

			r, err := strconv.ParseFloat(record[25], 64)
			if err != nil {
				log.WithError(err).Error("Failed conv r")
				continue
			}
			income, err := strconv.ParseFloat(record[8], 64)
			if err != nil {
				log.WithError(err).Error("Failed conv income")
				continue
			}


			m := models.Movie{
				Description: desc, Duration: duration, Year: year, Budget: budget,
				Tags: tags, Title: title, DirectorName: directorName, Rating: r,Income:income}
			movies = append(movies, m)

		}else {
			for ii, field := range record{
				fmt.Println(ii,field)
			}
		}
	}

	return movies, nil
}

func SendToES() error {
	ctx := context.Background()

	client, err := elastic.NewClient(
		elastic.SetURL("http://localhost:9200"), elastic.SetBasicAuth("elastic", "elastic"))
	if err != nil {
		return err
	}

	info, code, err := client.Ping("http://localhost:9200").Do(ctx)
	if err != nil {
		// Handle error
		return err
	}

	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	movies, err := readMovies("../dataset/movie_metadata.csv")
	if err != nil {
		return nil
	}
	for i, e := range movies {
		movie, err := json.Marshal(&e)
		if err != nil {
			return err
		}
		fmt.Println(i,string(movie))
		resp, err := client.Index().Index("movie").Type("_doc").BodyJson(string(movie)).Do(ctx)
		if err != nil {
			fmt.Printf("%d movie=%d was nos created. Error : %s \n", i, movie, err.Error())
			continue
		}
		fmt.Println(resp)

	}

	return nil
}
