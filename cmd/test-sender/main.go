package main

import (
	"fmt"
	"math/rand"
	"net"
	"time"
)

const (
	targetAddr = "localhost:5005"
)

var sampleMessages = []string{
	"!AIVDM,1,1,,A,13aG?00P00PF>@LKO80<@?nR0Lh0,0*1D",
	"!AIVDM,1,1,,A,15N8B?0P00PF>@LKO80<@?nR0Lh0,0*1C",
	"!AIVDM,1,1,,A,16?Bi50P00PF>@LKO80<@?nR0Lh0,0*1F",
	"!AIVDM,1,1,,A,33aG?00P00PF>@LKO80<@?nR0Lh0,0*1F",
	"!AIVDM,1,1,,A,35N8B?0P00PF>@LKO80<@?nR0Lh0,0*1E",
	"!AIVDM,1,1,,A,36?Bi50P00PF>@LKO80<@?nR0Lh0,0*19",
	"!AIVDM,1,1,,A,13u`Ea0P00PF>@LKO80<@?nR0Lh0,0*16",
	"!AIVDM,1,1,,A,15NwR@0P00PF>@LKO80<@?nR0Lh0,0*15",
	"!AIVDM,1,1,,A,16?Bi50P00PF>@LKO80<@?nR0Lh0,0*1F",
	"!AIVDM,1,1,,A,23aG?00P00PF>@LKO80<@?nR0Lh0,0*1C",
}

func main() {
	conn, err := net.Dial("udp", targetAddr)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Printf("Sending test AIS messages to %s...\n", targetAddr)
	fmt.Println("Press Ctrl+C to stop")

	ticker := time.NewTicker(10 * time.Millisecond)
	count := 0

	for range ticker.C {
		msg := sampleMessages[rand.Intn(len(sampleMessages))]
		_, err := conn.Write([]byte(msg + "\n"))
		if err != nil {
			fmt.Printf("Write error: %v\n", err)
			continue
		}
		count++
		if count%1000 == 0 {
			fmt.Printf("Sent %d messages\n", count)
		}
	}
}
