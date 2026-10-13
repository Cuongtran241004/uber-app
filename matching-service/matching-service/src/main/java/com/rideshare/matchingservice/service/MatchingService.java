package com.rideshare.matchingservice.service;

import com.rideshare.matchingservice.client.LocationServiceClient;
import com.rideshare.matchingservice.dto.NearByDriverResponse;
import com.rideshare.matchingservice.event.RideMatchedEvent;
import com.rideshare.matchingservice.event.RideRequestedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class MatchingService {
    private final LocationServiceClient locationServiceClient;
    private final KafkaTemplate<String, RideMatchedEvent> kakfaKafkaTemplate;

    private static final String RIDE_MATCHED_TOPIC = "ride.matched";
    private static final double DEFAULT_SEARCH_RADIUS_KM = 5.0;

    /**
     * Main matching algorithms
     * Called when RideRequestedEvent is consumed from Kafka
     *
     * STEPs:
     * 1. Ask Location Service for nearby drivers
     * 2. Score each driver and pick the best one
     * @param event
     */
    public void matchDriverForRide(RideRequestedEvent event){
        // STEP 1
        List<NearByDriverResponse> nearByDrivers = locationServiceClient
                .getNearByDrivers(
                        event.getPickupLatitude(),
                        event.getPickupLongitude(),
                        DEFAULT_SEARCH_RADIUS_KM
                );

        if(nearByDrivers.isEmpty()){
            log.warn("No nearby drivers found");
            return;
        }

        // STEP 2:
        Optional<NearByDriverResponse> bestDriver = findBestDriver(nearByDrivers);

        if (bestDriver.isEmpty()){
            log.warn("Could not find suitable driver for ride");
            return;
        }

        NearByDriverResponse assignedDriver = bestDriver.get();

        // STEP 3: Publish RideMatchedEvent to kafka
        RideMatchedEvent matchedEvent = new RideMatchedEvent(
                event.getRideId(),
                event.getRiderId(),
                assignedDriver.getDriverId(),
                assignedDriver.getLatitude(),
                assignedDriver.getLongitude(),
                assignedDriver.getDistanceInKm()
        );

        kakfaKafkaTemplate.send(RIDE_MATCHED_TOPIC, event.getRideId(), matchedEvent);
        log.info("Ride matched event sent to topic");
    }


    /**
     * Driver scoring algorithms
     *
     * Distance: 70%
     * Rating: 30%
     *
     * Score = (1 / distance) * distanceWeight + rating * ratingWeight
     */
    private Optional<NearByDriverResponse> findBestDriver(List<NearByDriverResponse> drivers){
        double distanceWeight = 0.7;
        double ratingWeight = 0.3;

        return drivers.stream()
                .max(Comparator.comparingDouble(driver -> {
                    // Distance score: closer = higher score
                    // Add 0.1 to avoid division by zero
                    double distanceScore = (1.0 / driver.getDistanceInKm() + 0.1);

                    // Simulate rating between 4.0 and 5.0
                    // In production: fetch from Driver Service
                    double simulatedRating = 4.0 + Math.random();

                    // Final weighted score
                    return (distanceScore * distanceWeight) +
                            (simulatedRating * ratingWeight);

                }));
    }
}
