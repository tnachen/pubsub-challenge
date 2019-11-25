package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

const (
	topicDeletedError = "topic is already deleted"
)

type Message interface {
	Data() string
}

type Producer interface {
	Publish(data string) error
}

type PubSub interface {
	Subscribe(topicPattern string) <-chan Message
	NewProducer(topic string) Producer
}

type ProducerImpl struct {
	queue *BoundedQueue
}

func (p ProducerImpl) Publish(data string) error {
	return p.queue.Publish(data)
}

type BoundedQueue struct {
	rwlock          sync.RWMutex
	deleted         bool
	matchedPatterns []string
	channels        []chan Message
}

type AggregatedQueue struct {
	incomingChannels []<-chan Message
	outgoingChannels []chan Message
	topicPattern     string
}

func (q *AggregatedQueue) Consumer() <-chan Message {
	newChannel := make(chan Message)
	q.outgoingChannels = append(q.outgoingChannels, newChannel)
	for _, incomingChannel := range q.incomingChannels {
		go func(in <-chan Message, out chan Message) {
			for message := range in {
				out <- message
			}
		}(incomingChannel, newChannel)
	}

	return newChannel
}

func NewAggregatedQueue(topicPattern string, channels []<-chan Message) *AggregatedQueue {
	return &AggregatedQueue{
		topicPattern:     topicPattern,
		incomingChannels: channels,
	}
}

func (q *AggregatedQueue) AddChannel(channel <-chan Message) {
	q.incomingChannels = append(q.incomingChannels, channel)
	for _, outgoingChannel := range q.outgoingChannels {
		go func(in <-chan Message, out chan Message) {
			for message := range in {
				out <- message
			}
		}(channel, outgoingChannel)
	}
}

type PubSubImpl struct {
	mutex    sync.Mutex
	queues   map[string]*BoundedQueue
	patterns map[string]*AggregatedQueue
}

type MessageImpl struct {
	data string
}

func (m MessageImpl) Data() string {
	return m.data
}

func NewBoundedQueue() *BoundedQueue {
	return &BoundedQueue{
		deleted:         false,
		matchedPatterns: make([]string, 0),
		channels:        make([]chan Message, 0),
	}
}

func (q *BoundedQueue) Consumer() <-chan Message {
	q.rwlock.Lock()
	defer q.rwlock.Unlock()
	channel := make(chan Message)
	q.channels = append(q.channels, channel)
	return channel
}

func (q *BoundedQueue) Publish(data string) error {
	q.rwlock.RLock()
	defer q.rwlock.RUnlock()

	if q.deleted {
		return errors.New(topicDeletedError)
	}

	message := MessageImpl{data: data}
	for _, channel := range q.channels {
		go func(c chan Message, m Message) {
			c <- m
		}(channel, message)
	}

	return nil
}

func (q *BoundedQueue) Delete() {
	q.rwlock.Lock()
	defer q.rwlock.Unlock()

	q.deleted = true
	for _, channel := range q.channels {
		close(channel)
	}
}

func NewPubSub() *PubSubImpl {
	return &PubSubImpl{
		queues:   make(map[string]*BoundedQueue),
		patterns: make(map[string]*AggregatedQueue),
	}
}

func (pubsub *PubSubImpl) findOrCreateQueue(topic string) *BoundedQueue {
	queue, ok := pubsub.queues[topic]
	if !ok {
		queue = NewBoundedQueue()
		pubsub.queues[topic] = queue
	}
	return queue
}

func (pubsub *PubSubImpl) Subscribe(topicPattern string) (<-chan Message, error) {
	pubsub.mutex.Lock()
	defer pubsub.mutex.Unlock()

	// Topic patterns without wildcards are only directed to one topic
	if !strings.Contains(topicPattern, "*") {
		return pubsub.findOrCreateQueue(topicPattern).Consumer(), nil
	}

	queue, ok := pubsub.patterns[topicPattern]
	if ok {
		return queue.Consumer(), nil
	}

	// Topic patterns with wildcard could hold one or more topics
	channels, err := pubsub.matchTopics(topicPattern)
	if err != nil {
		return nil, err
	}
	aggregatedQueue := NewAggregatedQueue(topicPattern, channels)
	pubsub.patterns[topicPattern] = aggregatedQueue

	return aggregatedQueue.Consumer(), nil
}

func (pubsub *PubSubImpl) NewProducer(topic string) (Producer, error) {
	pubsub.mutex.Lock()
	defer pubsub.mutex.Unlock()
	queue := pubsub.findOrCreateQueue(topic)
	for k, v := range pubsub.patterns {
		r, err := regexp.Compile(k)
		if err != nil {
			return nil, err
		}
		if r.MatchString(topic) {
			v.AddChannel(queue.Consumer())
		}
	}
	return ProducerImpl{queue: queue}, nil
}

func (pubsub *PubSubImpl) DeleteTopic(topic string) error {
	pubsub.mutex.Lock()
	defer pubsub.mutex.Unlock()

	queue, ok := pubsub.queues[topic]
	if !ok {
		return fmt.Errorf("topic %s is not found", topic)
	}

	queue.Delete()
	delete(pubsub.queues, topic)

	return nil
}

func (pubsub *PubSubImpl) matchTopics(topicPattern string) ([]<-chan Message, error) {
	r, err := regexp.Compile(topicPattern)
	if err != nil {
		return nil, err
	}
	var channels []<-chan Message
	for k, v := range pubsub.queues {
		if r.MatchString(k) {
			channels = append(channels, v.Consumer())
		}
	}

	return channels, nil
}
