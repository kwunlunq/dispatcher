package main

import (
	"testing"
)

func TestIntegration(t *testing.T) {
	type args struct {
		msgCount int
	}
	tests := []struct {
		name string
		args args
	}{
		{"5 Messages", args{5}},
		{"50 Messages", args{50}},
		{"1k Messages", args{1000}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReceived, gotErrCount, gotReplied := Integration(tt.args.msgCount)
			t.Logf("發送:%v  \t接收:%v  \t錯誤處理:%v  \t收到回條:%v", tt.args.msgCount, gotReceived, gotErrCount, gotReplied)
			if gotReceived < tt.args.msgCount || gotErrCount < tt.args.msgCount || gotReplied < tt.args.msgCount {
				t.Log("FAIL!")
				t.Fail()
			}
			t.Log("OK!")
		})
	}
	//_ = service.TopicService.Remove("dispatcher.example.testing", "dispatcher.example.testing_ERR", "dispatcher.example.testing_Reply")
}

func BenchmarkProducer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		send(topic, 1)
	}
}

func TestMultiConsProds(t *testing.T) {
	type args struct {
		msgCount int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{"10 Messages", args{10}, 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MultiPubSubs(tt.args.msgCount); got != tt.want {
				t.Errorf("MultiPubSubs() = %v, want %v", got, tt.want)
			}
		})
	}
}
