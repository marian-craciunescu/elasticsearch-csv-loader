package csvreader

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSendEarthquakes(t *testing.T) {
	a := assert.New(t)

	err := SendToElasticSearch()
	a.NoError(err)

}

func TestSendMovies(t *testing.T) {
	a := assert.New(t)

	err := SendToES()
	a.NoError(err)

}
