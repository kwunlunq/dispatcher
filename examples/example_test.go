package main

import (
	"testing"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/service"
)

func BenchmarkProducer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Producer(glob.Config.Topic, 1)
	}
	service.TopicService.Remove(glob.Config.Topic)
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
			if got := MultiConsProds(tt.args.msgCount); got != tt.want {
				t.Errorf("MultiConsProds() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIntegration(t *testing.T) {
	type args struct {
		msgCount int
	}
	tests := []struct {
		name         string
		args         args
		wantReceived int
		wantErrCount int
	}{
		{"5 Messages", args{5}, 5, 5},
		{"50 Messages", args{50}, 50, 50},
		{"50 Messages", args{500}, 500, 500},
		// {"50k Messages", args{50000}, 50000, 50000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReceived, gotErrCount := Integration(tt.args.msgCount)
			if gotReceived != tt.wantReceived || gotErrCount != tt.wantErrCount {
				t.Errorf("發送:%v 接收:%v 錯誤處理:%v", tt.args.msgCount, gotReceived, gotErrCount)
			}
		})
	}
	// time.Sleep(2 * time.Second)
	// service.TopicService.Remove("disp.testing", "disp.testing_ERR")
}
