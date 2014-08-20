// Copyright 2013 Ooyala, Inc.

/*
Package dogstatsd provides a Go DogStatsD client. DogStatsD extends StatsD - adding tags and
histograms. Refer to http://docs.datadoghq.com/guides/dogstatsd/ for information about DogStatsD.

Example Usage:
		// Create the client
		c, err := dogstatsd.New("127.0.0.1:8125")
		defer c.Close()
		if err != nil {
			log.Fatal(err)
		}
		// Prefix every metric with the app name
		c.Namespace = "flubber."
		// Send the EC2 availability zone as a tag with every metric
		append(c.Tags, "us-east-1a")
		err = c.Gauge("request.duration", 1.2, nil, 1)

dogstatsd is based on go-statsd-client.
*/
package dogstatsd

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"strconv"
)

var (
	MetricSeparator = []byte{':'}
	RateSeparator   = []byte("|@")
	TagSeparator    = []byte("|#")
	GaugeSpec       = []byte("|g")
	CountSpec       = []byte("|c")
	HistogramSpec   = []byte("|h")
	TimerSpec       = []byte("|ms")
	SetSpec         = []byte("|s")
	Comma           = []byte{','}
)

// Client holds onto a connection and the other context necessary for every stasd packet.
type Client struct {
	conn net.Conn
	// Namespace to prepend to all statsd calls
	Namespace string
	// Global tags to be added to every statsd call
	Tags []string
}

// New returns a pointer to a new Client and an error.
// addr must have the format "hostname:port"
func New(addr string) (*Client, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	client := &Client{conn: conn}
	return client, nil
}

// Close closes the connection to the DogStatsD agent
func (c *Client) Close() error {
	return c.conn.Close()
}

// send handles sampling and sends the message over UDP. It also adds global namespace prefixes and tags.
func (c *Client) send(b *bytes.Buffer, spec []byte, tags []string, rate float64) error {
	if _, err := b.Write(spec); err != nil {
		return err
	}

	if rate < 1 {
		if rand.Float64() < rate {
			if _, err := b.Write(RateSeparator); err != nil {
				return err
			}
			/*
				bs := b.Bytes()
				bs = strconv.AppendFloat(bs, rate, 'f', -1, 64)
				b = bytes.NewBuffer(bs)
			*/
			b.WriteString(strconv.FormatFloat(rate, 'f', -1, 64))
		} else {
			return nil
		}
	}

	tags = append(c.Tags, tags...)
	if len(tags) > 0 {
		if _, err := b.Write(TagSeparator); err != nil {
			return err
		}
		l := len(tags) - 1
		for i, t := range tags {
			if _, err := b.WriteString(t); err != nil {
				return err
			}
			if i != l {
				if _, err := b.Write(Comma); err != nil {
					return err
				}
			}
		}
	}

	_, err := c.conn.Write(b.Bytes())
	return err
}

// Event posts to the Datadog event stream.
func (c *Client) Event(title string, text string, tags []string) error {
	var b bytes.Buffer

	fmt.Fprintf(&b, "_e{%d,%d}:%s|%s", len(title), len(text), title, text)
	tags = append(c.Tags, tags...)
	format := "|#%s"
	for _, t := range tags {
		fmt.Fprintf(&b, format, t)
		format = ",%s"
	}

	_, err := c.conn.Write(b.Bytes())
	return err
}

func (c *Client) start(b *bytes.Buffer, name string) error {
	var err error
	if _, err = b.WriteString(c.Namespace); err != nil {
		return err
	}
	if _, err = b.WriteString(name); err != nil {
		return err
	}
	if _, err = b.Write(MetricSeparator); err != nil {
		return err
	}
	return nil
}

// Gauge measures the value of a metric at a particular time
func (c *Client) Gauge(name string, value float64, tags []string, rate float64) error {
	var b bytes.Buffer
	if err := c.start(&b, name); err != nil {
		return err
	}
	if _, err := b.WriteString(strconv.FormatFloat(value, 'f', -1, 64)); err != nil {
		return err
	}
	return c.send(&b, GaugeSpec, tags, rate)
}

// Count tracks how many times something happened per second
func (c *Client) Count(name string, value int64, tags []string, rate float64) error {
	var b bytes.Buffer
	if err := c.start(&b, name); err != nil {
		return err
	}
	if _, err := b.WriteString(strconv.FormatInt(value, 10)); err != nil {
		return err
	}
	return c.send(&b, CountSpec, tags, rate)
}

// Histogram tracks the statistical distribution of a set of values
func (c *Client) Histogram(name string, value float64, tags []string, rate float64) error {
	var b bytes.Buffer
	if err := c.start(&b, name); err != nil {
		return err
	}
	if _, err := b.WriteString(strconv.FormatFloat(value, 'f', -1, 64)); err != nil {
		return err
	}
	return c.send(&b, HistogramSpec, tags, rate)
}

// Timer tracks the statistical distribution of a set of durations
func (c *Client) Timer(name string, value float64, tags []string, rate float64) error {
	var b bytes.Buffer
	if err := c.start(&b, name); err != nil {
		return err
	}
	if _, err := b.WriteString(strconv.FormatFloat(value, 'f', -1, 64)); err != nil {
		return err
	}
	return c.send(&b, TimerSpec, tags, rate)
}

// Set counts the number of unique elements in a group
func (c *Client) Set(name string, value string, tags []string, rate float64) error {
	var b bytes.Buffer
	if err := c.start(&b, name); err != nil {
		return err
	}
	if _, err := b.WriteString(value); err != nil {
		return err
	}
	return c.send(&b, SetSpec, tags, rate)
}
