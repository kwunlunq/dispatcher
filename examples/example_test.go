package main

import (
	"testing"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/service"
)

func TestIntegration(t *testing.T) {
	type args struct {
		msgCount int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{"10 Messages", args{10}, 10},
		{"15 Messages", args{15}, 15},
		// {"50k Messages", args{50000}, 50000},
		// {"500k Messages", args{500000}, 500000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Integration(tt.args.msgCount); got != tt.want {
				t.Errorf("Integration() = %v, want %v", got, tt.want)
			} else {
				t.Log("Great")
			}
		})
	}
	service.TopicService.Remove(glob.Config.Topic)
}

func BenchmarkProducer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Producer(glob.Config.Topic, 1)
	}
	service.TopicService.Remove(glob.Config.Topic)
}
