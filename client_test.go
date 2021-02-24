package statsd

import (
	"net"
	"reflect"
	"strings"
	"testing"
)

var udpConnectionStubIO chan []byte = make(chan []byte)

type udpConnectionStub struct {
	net.Conn
}

func (stub *udpConnectionStub) Write(p []byte) (n int, err error) {
	udpConnectionStubIO <- p
	n = 0
	err = nil
	return
}

func TestNewClient(t *testing.T) {
	client := NewClient("127.0.0.1", 9876)

	// check type
	typeName := reflect.TypeOf(*client).Name()
	if typeName != "Client" {
		t.Errorf("Wrong type of factory method return value: \"%s\"", typeName)
	}

	// check buffered mode
	if client.keyBuffer != nil {
		t.Errorf("Buffer must be disabled")
	}
}

func TestNewBufferedClient(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)

	// check type
	typeName := reflect.TypeOf(*client).Name()
	if typeName != "Client" {
		t.Errorf("Wrong type of factory method return value: \"%s\"", typeName)
	}

	// check buffered mode
	if client.keyBuffer == nil {
		t.Errorf("Buffer must be enabled")
	}
}

func TestOpen(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)

	if client.conn != nil {
		t.Errorf("Connection must be nill")
	}

	client.Open()

	if _, ok := client.conn.(net.Conn); !ok {
		t.Errorf("Connection must be instanc e of net.Conn")
	}
}

func TestClose(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)

	if client.conn != nil {
		t.Errorf("Connection must be nill")
	}

	client.Open()
	client.Close()

	if client.conn != nil {
		t.Errorf("Connection must be closed")
	}
}

func TestBufferedClient_Timing_BigRate(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)
	client.Timing("a.b.c", 320, 0.9999)

	if len(client.keyBuffer) > 0 && client.keyBuffer[0] != "a.b.c:320|ms|@0.9999" {
		t.Errorf("Wrong timing metric with big rate: \"%s\"", client.keyBuffer[0])
	}
}

func TestBufferedClient_Timing_SmallRate(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)
	client.Timing("a.b.c", 320, 0.0001)

	if len(client.keyBuffer) > 0 && client.keyBuffer[0] != "a.b.c:320|ms|@0.0001" {
		t.Errorf("Wrong timing metric with small rate: \"%s\"", client.keyBuffer[0])
	}
}

func TestBufferedClient_Timing_NoRate(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)
	client.Timing("a.b.c", 320, 1)

	if client.keyBuffer[0] != "a.b.c:320|ms" {
		t.Errorf("Wrong timing metric without rate: \"%s\"", client.keyBuffer[0])
	}
}

func TestBufferedClient_Count_BigRate(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)
	client.Count("a.b.c", 320, 0.9999)

	if len(client.keyBuffer) > 0 && client.keyBuffer[0] != "a.b.c:320|c|@0.9999" {
		t.Errorf("Wrong count metric with big rate: \"%s\"", client.keyBuffer[0])
	}
}

func TestBufferedClient_Count_SmallRate(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)
	client.Count("a.b.c", 320, 0.0001)

	if len(client.keyBuffer) > 0 && client.keyBuffer[0] != "a.b.c:320|c|@0.0001" {
		t.Errorf("Wrong count metric with small rate: \"%s\"", client.keyBuffer[0])
	}
}

func TestBufferedClient_Count_NoRate(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)
	client.Count("a.b.c", 320, 1)

	if client.keyBuffer[0] != "a.b.c:320|c" {
		t.Errorf("Wrong count metric without rate: \"%s\"", client.keyBuffer[0])
	}
}

func TestBufferedClient_MetricPrefix_Empty(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)
	client.Count("a.b.c", 320, 1)

	if client.keyBuffer[0] != "a.b.c:320|c" {
		t.Errorf("Wrong count metric without rate: \"%s\"", client.keyBuffer[0])
	}
}

func TestBufferedClient_MetricPrefix_NotEmptyNoDot(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)
	client.SetPrefix("somePrefix")
	client.Count("a.b.c", 320, 1)

	if client.keyBuffer[0] != "somePrefix.a.b.c:320|c" {
		t.Errorf("Wrong count metric without rate: \"%s\"", client.keyBuffer[0])
	}
}

func TestBufferedClient_MetricPrefix_NotEmptyWithDot(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)
	client.SetPrefix("somePrefix.")
	client.Count("a.b.c", 320, 1)

	if client.keyBuffer[0] != "somePrefix.a.b.c:320|c" {
		t.Errorf("Wrong count metric without rate: \"%s\"", client.keyBuffer[0])
	}
}

func TestBufferedClient_Gauge(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)
	client.Gauge("a.b.c", 320)

	if client.keyBuffer[0] != "a.b.c:320|g" {
		t.Errorf("Wrong gauge metric: \"%s\"", client.keyBuffer[0])
	}
}

func TestBufferedClient_GaugeShift(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)

	client.GaugeShift("a.b.c", 320)
	if client.keyBuffer[0] != "a.b.c:+320|g" {
		t.Errorf("Wrong positive gauge shift metric: \"%s\"", client.keyBuffer[0])
	}

	client.GaugeShift("a.b.c", -320)
	if client.keyBuffer[1] != "a.b.c:-320|g" {
		t.Errorf("Wrong negative gauge shift metric: \"%s\"", client.keyBuffer[1])
	}
}

func TestBufferedClient_Set(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)
	client.Set("a.b.c", 320)

	if client.keyBuffer[0] != "a.b.c:320|s" {
		t.Errorf("Wrong set metric: \"%s\"", client.keyBuffer[0])
	}
}

func TestBufferedClient_addToBuffer(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)
	client.addToBuffer("a.b.c", "320|s")
	client.addToBuffer("a.b.d", "321|ms|@0.0001")

	if len(client.keyBuffer) != 2 {
		t.Errorf("Must be 2 keys in buffer")
	}

	if client.keyBuffer[0] != "a.b.c:320|s" {
		t.Errorf("Wrong metric added to buffer: \"%s\"", client.keyBuffer[0])
	}

	if client.keyBuffer[1] != "a.b.d:321|ms|@0.0001" {
		t.Errorf("Wrong metric added to buffer: \"%s\"", client.keyBuffer[1])
	}
}

func TestBufferedClient_isSendAcceptedBySampleRate(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)

	if client.isSendAcceptedBySampleRate(2) == false {
		t.Errorf("2 must be accepred by sample rate")
	}

	if client.isSendAcceptedBySampleRate(1) == false {
		t.Errorf("1 must be accepred by sample rate")
	}

	if client.isSendAcceptedBySampleRate(0.00000001) == true {
		t.Errorf("0.00000001 must not be accepred by sample rate")
	}

	if client.isSendAcceptedBySampleRate(0.99999999) == false {
		t.Errorf("0.99999999 must be accepred by sample rate")
	}
}

func TestBufferedClient_Flush(t *testing.T) {
	client := NewBufferedClient("127.0.0.1", 9876)

	client.conn = new(udpConnectionStub)

	err := client.Flush()
	if err != nil {
		t.Errorf("Flush fails with error: %s", err)
	}

	client.Count("a.a", 42, 1)
	client.Timing("a.b", 43, 1)
	client.Gauge("a.c", 44)
	client.GaugeShift("a.d", 45)
	client.GaugeShift("a.e", 46)
	client.Set("a.f", 47)

	err = client.Flush()
	if err != nil {
		t.Errorf("Flush fails with error: %s", err)
	}

	metricPacketBytes := <-udpConnectionStubIO
	actualMetricPacket := strings.Replace(
		string(metricPacketBytes),
		"\n",
		"@",
		-1,
	)

	expectedMetricPacket := "a.a:42|c@a.b:43|ms@a.c:44|g@a.d:+45|g@a.e:+46|g@a.f:47|s"

	if expectedMetricPacket != actualMetricPacket {
		t.Errorf(
			"Wrong metric packet send by buffered client: %s, expected: %s",
			actualMetricPacket,
			expectedMetricPacket,
		)
	}
}

func TestClient_Flush(t *testing.T) {
	client := NewClient("127.0.0.1", 9876)
	err := client.Flush()

	if err == nil {
		t.Errorf("Flush in unbuffered mode must return error")
	}

	if err.Error() != "Invalid call of flush in unbuffered mode" {
		t.Errorf("Invalid error on flush in unbuffered mode")
	}
}

func TestClient_Send(t *testing.T) {
	client := NewClient("127.0.0.1", 9876)

	client.conn = new(udpConnectionStub)

	// count
	client.Count("a.a", 42, 1)
	actualCountMetricPacketBytes := <-udpConnectionStubIO
	expectedCountMetricPacket := "a.a:42|c"
	if expectedCountMetricPacket != string(actualCountMetricPacketBytes) {
		t.Errorf(
			"Wrong count metric packet send by unbuffered client: %s, expected: %s",
			string(actualCountMetricPacketBytes),
			expectedCountMetricPacket,
		)
	}

	// timing
	client.Timing("a.b", 43, 1)
	actualTimingMetricPacketBytes := <-udpConnectionStubIO
	expectedTimingMetricPacket := "a.b:43|ms"
	if expectedTimingMetricPacket != string(actualTimingMetricPacketBytes) {
		t.Errorf(
			"Wrong timing metric packet send by unbuffered client: %s, expected: %s",
			string(actualTimingMetricPacketBytes),
			expectedTimingMetricPacket,
		)
	}
}
