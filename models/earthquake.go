package models

import "time"

//DateTime,Latitude,Longitude,Depth,Magnitude,MagType,NbStations,Gap,Distance,RMS,Source,EventID
//1970/01/04 17:00:40.20,24.138999999999900,102.503000000000000,31.00,7.50,Ms,90,,,0.000000000000000,NEI,1970010440
type EarthQuake struct {
	Time *time.Time
	Latitude float64
	Longitude float64
	Depth float64
	Magnitude float64
	MagType string
	NbStations int64
	Gap string
	Distance float64
	RMS float64
	Source string
	EventID string
	Month string
}