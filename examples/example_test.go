package main

import (
	"testing"
	"time"
)

func TestIntegration(t *testing.T) {
	type args struct {
		msgCount      int
		producerCount int
		consumerCount int
	}
	tests := []struct {
		name string
		args args
	}{
		{"5 Messages, 1 x 1", args{5, 1, 1}},
		{"50 Messages 1 x 1", args{50, 1, 1}},
		{"1k Messages 1 x 1", args{1000, 1, 1}},
		{"100 Messages 1 x 2", args{100, 1, 2}},
		{"100 Messages 2 x 1", args{100, 2, 1}},
		{"100 Messages 2 x 2", args{100, 2, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReceived, gotErrCount, gotReplied, expectedReceived := Integration(tt.args.msgCount, tt.args.producerCount, tt.args.consumerCount)
			t.Logf("發送:%v  \t接收:%v  \t錯誤處理:%v  \t收到回條:%v", tt.args.msgCount, gotReceived, gotErrCount, gotReplied)
			time.Sleep(time.Second)
			if gotReceived < expectedReceived || gotErrCount < expectedReceived || gotReplied < expectedReceived {
				t.Log("FAIL!")
				t.Fail()
			}
			t.Log("OK!")
		})
	}
}

func BenchmarkProducer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		send(1, 1)
	}
}
