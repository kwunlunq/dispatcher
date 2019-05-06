package main

import (
	"testing"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/service"
)

func TestIntegration(t *testing.T) {
	// testCount := settings.Config.GetValueAsInt(glob.ProjName, "test_count", 5)
	type args struct {
		msgCount int
	}
	tests := []struct {
		name string
		args args
	}{
		{"5 Messages", args{5}},
		{"50 Messages", args{50}},
		{"500 Messages", args{500}},
		// {strconv.Itoa(testCount) + " Messages", args{testCount}, testCount, testCount},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReceived, gotErrCount := Integration(tt.args.msgCount)
			t.Logf("發送:%v  \t接收:%v  \t錯誤處理:%v", tt.args.msgCount, gotReceived, gotErrCount)
			if gotReceived < tt.args.msgCount || gotErrCount < tt.args.msgCount {
				t.Fail()
				// t.Errorf("發送:%v 接收:%v 錯誤處理:%v", tt.args.msgCount, gotReceived, gotErrCount)
			}
		})
	}
	service.TopicService.Remove("disp.testing", "disp.testing_ERR")
}

func BenchmarkProducer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Producer(glob.Config.Topic, 1)
	}
	// service.TopicService.Remove(glob.Config.Topic)
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
