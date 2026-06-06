package model

import "time"

type AISMessage struct {
	Raw        string    `json:"raw"`
	Timestamp  time.Time `json:"timestamp"`
	PacketType string    `json:"packet_type"`
}

type VesselData struct {
	MMSI       uint32    `json:"mmsi"`
	Latitude   float64   `json:"latitude"`
	Longitude  float64   `json:"longitude"`
	COG        float64   `json:"cog"`
	SOG        float64   `json:"sog"`
	Timestamp  time.Time `json:"timestamp"`
	MessageType uint8    `json:"message_type"`
}

type ParsedMessage struct {
	Data   *VesselData
	Error  error
	Source string
}
