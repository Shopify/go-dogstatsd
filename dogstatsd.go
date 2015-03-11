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
		c.SetNamespace("flubber.")
		// Send the EC2 availability zone as a tag with every metric
		c.SetTags([]string{"us-east-1a"})

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
	"sync"
	"time"
)

var (
	metricSeparator = []byte{':'}
	rateSeparator   = []byte("|@")
	tagSeparator    = []byte("|#")
	gaugeSpec       = []byte("|g")
	countSpec       = []byte("|c")
	histogramSpec   = []byte("|h")
	timerSpec       = []byte("|ms")
	setSpec         = []byte("|s")
	comma           = []byte{','}
)

type Config struct {
	l         sync.Mutex
	Namespace string
	Tags      []string
}

func (c *Config) Clone() *Config {
	cc := &Config{}

	c.l.Lock()
	defer c.l.Unlock()

	cc.l.Lock()
	defer cc.l.Unlock()

	cc.Namespace = c.Namespace
	cc.Tags = make([]string, len(c.Tags))
	copy(cc.Tags, c.Tags)

	return cc
}

// Client holds onto a connection and the other context necessary for every stasd packet.
type Client struct {
	conn   net.Conn
	config *Config
}

// New returns a pointer to a new Client and an error.
// addr must have the format "hostname:port"
func New(addr string, config *Config) (*Client, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}

	if config == nil {
		config = &Config{}
	}

	client := &Client{conn: conn, config: config.Clone()}
	return client, nil
}

// Close closes the connection to the DogStatsD agent
func (c *Client) Close() error {
	return c.conn.Close()
}

func Clone(current *Client, newConfig *Config) *Client {
	if newConfig == nil {
		newConfig = current.config
	}

	return &Client{conn: current.conn, config: newConfig}
}

// send handles sampling and sends the message over UDP. It also adds global namespace prefixes and tags.
func (c *Client) send(b *bytes.Buffer, spec []byte, tags []string, rate float64) error {
	if _, err := b.Write(spec); err != nil {
		return err
	}

	if rate < 1 {
		if rand.Float64() < rate {
			if _, err := b.Write(rateSeparator); err != nil {
				return err
			}
			b.WriteString(strconv.FormatFloat(rate, 'f', -1, 64))
		} else {
			return nil
		}
	}

	tags = append(c.config.Tags, tags...)
	if len(tags) > 0 {
		if _, err := b.Write(tagSeparator); err != nil {
			return err
		}
		l := len(tags) - 1
		for i, t := range tags {
			if _, err := b.WriteString(t); err != nil {
				return err
			}
			if i != l {
				if _, err := b.Write(comma); err != nil {
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
	tags = append(c.config.Tags, tags...)
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
	if _, err = b.WriteString(c.config.Namespace); err != nil {
		return err
	}
	if _, err = b.WriteString(name); err != nil {
		return err
	}
	if _, err = b.Write(metricSeparator); err != nil {
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
	return c.send(&b, gaugeSpec, tags, rate)
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
	return c.send(&b, countSpec, tags, rate)
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
	return c.send(&b, histogramSpec, tags, rate)
}

// Timer tracks the statistical distribution of a set of durations
func (c *Client) Timer(name string, duration time.Duration, tags []string, rate float64) error {
	var b bytes.Buffer
	if err := c.start(&b, name); err != nil {
		return err
	}
	durationInMs := float64(duration) / float64(time.Millisecond)
	if _, err := b.WriteString(strconv.FormatFloat(durationInMs, 'f', -1, 64)); err != nil {
		return err
	}
	return c.send(&b, timerSpec, tags, rate)
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
	return c.send(&b, setSpec, tags, rate)
}
