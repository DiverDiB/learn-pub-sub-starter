package routing

import "time"

type PlayingState struct {
	IsPaused bool
}

type GameLog struct {
	Username    string
	CurrentTime time.Time
	Message     string
}
