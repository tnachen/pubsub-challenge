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
	queue *SingleTopicPublisher
}

func (p ProducerImpl) Publish(data string) error {
	return p.queue.Publish(data)
}

type SingleTopicPublisher struct {
	rwlock          sync.RWMutex
	deleted         bool
	matchedPatterns []string
	channels        []chan Message
}

type WildcardTopicPublisher struct {
	incomingChannels []<-chan Message
	outgoingChannels []chan Message
	topicPattern     string
}

func (q *WildcardTopicPublisher) Consumer() <-chan Message {
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

func NewWildcardTopicPublisher(topicPattern string, channels []<-chan Message) *WildcardTopicPublisher {
	return &WildcardTopicPublisher{
		topicPattern:     topicPattern,
		incomingChannels: channels,
	}
}

func (q *WildcardTopicPublisher) AddChannel(channel <-chan Message) {
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
	queues   map[string]*SingleTopicPublisher
	patterns map[string]*WildcardTopicPublisher
}

type MessageImpl struct {
	data string
}

func (m MessageImpl) Data() string {
	return m.data
}

func NewSingleTopicPublisher() *SingleTopicPublisher {
	return &SingleTopicPublisher{
		deleted:         false,
		matchedPatterns: make([]string, 0),
		channels:        make([]chan Message, 0),
	}
}

func (q *SingleTopicPublisher) Consumer() <-chan Message {
	q.rwlock.Lock()
	defer q.rwlock.Unlock()
	channel := make(chan Message)
	q.channels = append(q.channels, channel)
	return channel
}

func (q *SingleTopicPublisher) Publish(data string) error {
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

func (q *SingleTopicPublisher) Delete() {
	q.rwlock.Lock()
	defer q.rwlock.Unlock()

	q.deleted = true
	for _, channel := range q.channels {
		close(channel)
	}
}

func NewPubSub() *PubSubImpl {
	return &PubSubImpl{
		queues:   make(map[string]*SingleTopicPublisher),
		patterns: make(map[string]*WildcardTopicPublisher),
	}
}

func (pubsub *PubSubImpl) findOrCreateQueue(topic string) *SingleTopicPublisher {
	queue, ok := pubsub.queues[topic]
	if !ok {
		queue = NewSingleTopicPublisher()
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
	aggregatedQueue := NewWildcardTopicPublisher(topicPattern, channels)
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
