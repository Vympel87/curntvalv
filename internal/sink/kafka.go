package sink

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/Vympel87/curntvalv/internal/event"
	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Kafka produces changes to Kafka topics with franz-go (design D20), one
// record per change: the value is the shared JSON form, the key the row's
// replica identity key as a JSON object ({"id":"5"}), so the changes of a
// row stay in one partition and in order, and the headers carry
// curntvalv-id (the change id) and curntvalv-op. The topic is Topic with
// "{table}" replaced by "namespace.name" ("_source_reset" for a source
// reset). A change is delivered when every in-sync replica has it.
//
// The producer is idempotent, so its own retries never duplicate a record;
// after a leader change of the curntValv cluster a new producer may send
// some changes again, and consumers de-duplicate on curntvalv-id.
type Kafka struct {
	brokers []string
	shown   string
	topic   string
	tls     *tls.Config
	mech    sasl.Mechanism
	timeout time.Duration

	mu     sync.Mutex
	client *kgo.Client
}

// KafkaConfig configures a Kafka sink.
type KafkaConfig struct {
	// URLs are kafka://[USER:PASSWORD@]HOST:PORT (kafka+tls:// for TLS), one
	// or more seed brokers; credentials are taken from the first.
	URLs []string
	// SASL is the mechanism for credentials: "plain" (default),
	// "scram-sha-256" or "scram-sha-512".
	SASL string
	// Topic may contain "{table}". Default "curntvalv.{table}".
	Topic   string
	Timeout time.Duration
}

// NewKafka returns a Kafka sink. It does no I/O.
func NewKafka(cfg KafkaConfig) (*Kafka, error) {
	if len(cfg.URLs) == 0 {
		return nil, errors.New("kafka: no broker url")
	}
	k := &Kafka{topic: cfg.Topic, timeout: cfg.Timeout}
	var shown []string
	for i, raw := range cfg.URLs {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "kafka" && u.Scheme != "kafka+tls") || u.Host == "" {
			return nil, errors.New("kafka: bad url (want kafka://[USER:PASSWORD@]HOST:PORT or kafka+tls://...)")
		}
		host := u.Host
		if u.Port() == "" {
			host = net.JoinHostPort(u.Hostname(), "9092")
		}
		k.brokers = append(k.brokers, host)
		shown = append(shown, u.Redacted())
		if i > 0 {
			continue
		}
		if u.Scheme == "kafka+tls" {
			k.tls = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		if u.User != nil {
			user := u.User.Username()
			pass, _ := u.User.Password()
			switch cfg.SASL {
			case "", "plain":
				k.mech = plain.Auth{User: user, Pass: pass}.AsMechanism()
			case "scram-sha-256":
				k.mech = scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()
			case "scram-sha-512":
				k.mech = scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()
			default:
				return nil, fmt.Errorf("kafka: unknown SASL mechanism %q (plain, scram-sha-256, scram-sha-512)", cfg.SASL)
			}
		}
	}
	if cfg.SASL != "" && k.mech == nil {
		return nil, errors.New("kafka: sasl needs USER:PASSWORD in the first url")
	}
	k.shown = strings.Join(shown, ",")
	if k.topic == "" {
		k.topic = "curntvalv.{table}"
	}
	if strings.ContainsFunc(strings.ReplaceAll(k.topic, "{table}", ""), func(r rune) bool { return !topicChar(r) }) {
		return nil, fmt.Errorf("kafka: bad topic %q (letters, digits, '.', '_', '-' and {table})", k.topic)
	}
	if k.timeout <= 0 {
		k.timeout = 30 * time.Second
	}
	return k, nil
}

func topicChar(r rune) bool {
	return r == '.' || r == '_' || r == '-' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
}

// topicOf returns the topic of a change; characters Kafka does not allow
// become '_'.
func (k *Kafka) topicOf(c event.Change) string {
	t := c.Table()
	if c.IsSourceReset() {
		t = "_source_reset"
	}
	t = strings.Map(func(r rune) rune {
		if topicChar(r) {
			return r
		}
		return '_'
	}, t)
	name := strings.ReplaceAll(k.topic, "{table}", t)
	if len(name) > 249 {
		name = name[:249]
	}
	return name
}

// recordKey returns the row's key columns as a JSON object, or nil (no key
// columns, truncate, source reset).
func recordKey(c event.Change) ([]byte, error) {
	s := c.Schema
	row := c.Event.GetAfter()
	if c.Event.GetOp() == pb.Op_OP_DELETE {
		row = c.Event.GetBefore()
	}
	if s == nil || row == nil {
		return nil, nil
	}
	key := map[string]any{}
	for i, col := range s.GetColumns() {
		if !col.GetKey() || i >= len(row.GetValues()) {
			continue
		}
		v := row.GetValues()[i]
		switch v.GetKind() {
		case pb.ValueKind_VALUE_KIND_TEXT:
			key[col.GetName()] = string(v.GetData())
		case pb.ValueKind_VALUE_KIND_NULL:
			key[col.GetName()] = nil
		default:
			key[col.GetName()] = v.GetData()
		}
	}
	if len(key) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(key)
	if err != nil {
		return nil, fmt.Errorf("kafka: encode key: %w", err)
	}
	return b, nil
}

// Deliver produces all changes and waits until every one is acknowledged.
func (k *Kafka) Deliver(ctx context.Context, changes []event.Change) error {
	if len(changes) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, len(changes))
	for i, c := range changes {
		v, err := json.Marshal(c.JSON())
		if err != nil {
			return fmt.Errorf("kafka: encode: %w", err)
		}
		key, err := recordKey(c)
		if err != nil {
			return err
		}
		recs[i] = &kgo.Record{Topic: k.topicOf(c), Key: key, Value: v, Headers: []kgo.RecordHeader{
			{Key: "curntvalv-id", Value: []byte(c.ID())}, {Key: "curntvalv-op", Value: []byte(c.Op())},
		}}
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	cl, err := k.connect()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		// Start a new producer next time rather than reason about the state
		// of this one.
		cl.Close()
		k.client = nil
		return fmt.Errorf("kafka: produce to %s: %w", k.shown, err)
	}
	return nil
}

// connect returns the client, creating it on first use. Creating it does
// not contact the brokers yet.
func (k *Kafka) connect() (*kgo.Client, error) {
	if k.client != nil {
		return k.client, nil
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(k.brokers...),
		kgo.ClientID("curntvalv"),
		kgo.RequiredAcks(kgo.AllISRAcks()), // also needed by the idempotent producer
		kgo.AllowAutoTopicCreation(),       // if the brokers allow it
		kgo.RecordDeliveryTimeout(k.timeout),
		kgo.ProducerLinger(0), // a batch is produced at once
		kgo.DialTimeout(10 * time.Second),
	}
	if k.tls != nil {
		opts = append(opts, kgo.DialTLSConfig(k.tls.Clone()))
	}
	if k.mech != nil {
		opts = append(opts, kgo.SASL(k.mech))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}
	k.client = cl
	return cl, nil
}

// Close closes the producer.
func (k *Kafka) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.client != nil {
		k.client.Close()
		k.client = nil
	}
	return nil
}
