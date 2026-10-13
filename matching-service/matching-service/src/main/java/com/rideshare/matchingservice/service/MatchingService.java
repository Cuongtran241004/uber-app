package com.rideshare.matchingservice.service;

import com.rideshare.matchingservice.client.LocationServiceClient;
import com.rideshare.matchingservice.dto.NearByDriverResponse;
import com.rideshare.matchingservice.event.RideMatchedEvent;
import com.rideshare.matchingservice.event.RideRequestedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

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
     * @param event
     */
    public void matchDriverForRide(RideRequestedEvent event){
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
    }
}
