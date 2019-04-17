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
		want int
	}{
		{"10 Messages", args{10}, 10},
		{"15 Messages", args{15}, 15},
		// {"20 Messages", args{20}, 20},
		// {"500 Messages", args{500}, 500},
		// {"5000 Messages", args{5000}, 5000},
		{"50000 Messages", args{50000}, 50000},
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
	// service.TopicService.Remove(glob.Config.Topic)
}

// func TestProducer(t *testing.T) {
// 	type args struct {
// 		count int
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 	}{
// 		{"5000 Messages", args{5000}},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			Producer(tt.args.count)
// 		})
// 	}
// 	service.TopicService.Remove(glob.Config.Topic)
// }
