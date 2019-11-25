package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPubsubOneProducerTwoConsumers(t *testing.T) {
	ps := NewPubSub()
	producer, err := ps.NewProducer("topicA")
	assert.NoError(t, err)
	c1, err := ps.Subscribe("topicA")
	assert.NoError(t, err)
	c2, err := ps.Subscribe("topicA")
	assert.NoError(t, err)
	producer.Publish("a1")
	assert.Equal(t, "a1", (<-c1).Data())
	assert.Equal(t, "a1", (<-c2).Data())
	producer.Publish("a2")
	assert.Equal(t, "a2", (<-c1).Data())
	assert.Equal(t, "a2", (<-c2).Data())
}

func TestPubsubWildcard(t *testing.T) {
	ps := NewPubSub()
	producer1, err := ps.NewProducer("cosmos-eventA-topicA")
	c1, err := ps.Subscribe("cosmos-event*")
	assert.NoError(t, err)
	c2, err := ps.Subscribe("cosmos-eventA-topicA")
	assert.NoError(t, err)
	producer2, err := ps.NewProducer("cosmos-eventA-topicB")
	assert.NoError(t, err)
	c3, err := ps.Subscribe("cosmos-eventA-topicB")
	assert.NoError(t, err)
	producer1.Publish("topicA-1")
	producer2.Publish("topicB-1")
	assert.Equal(t, "topicA-1", (<-c2).Data())
	assert.Equal(t, "topicB-1", (<-c3).Data())
	m1 := (<-c1).Data()
	m2 := (<-c1).Data()
	assert.True(t, m1 == "topicA-1" || m1 == "topicB-1")
	assert.True(t, m2 == "topicA-1" || m2 == "topicB-1")
}
