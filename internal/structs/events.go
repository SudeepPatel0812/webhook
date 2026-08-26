package structs

type Event struct {
	IdempotencyKey string                 `json:"-"`
	ApplicationId  string                 `json:"application_id"`
	EventType      string                 `json:"event_type"`
	Payload        map[string]interface{} `json:"payload"`
}
