package com.rideshare.matchingservice.service;


import com.rideshare.matchingservice.event.RideRequestedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class RideEventConsumer {
    private final MatchingService matchingService;

    /**
     * Listen to ride.requested
     * Triggered every time Ride Service published a new ride request
     *
     * FLOWs:
     * Ride Service -> Kafka(ride.requested) -> this consumer -> Matching Service
     */
    @KafkaListener(
                topics = "ride.requested",
                groupId = "matching-service-group")
    public void consumeRequestedRide(RideRequestedEvent event){
        try {
            matchingService.matchDriverForRide(event);

        } catch (Exception e) {
            log.error("Consume Ride Event Error", e.getMessage());

            // In production: send to dead letter for retry

        }
    }
}
