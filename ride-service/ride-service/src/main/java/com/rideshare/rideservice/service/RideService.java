package com.rideshare.rideservice.service;

import com.rideshare.rideservice.dto.RideRequest;
import com.rideshare.rideservice.dto.RideResponse;
import com.rideshare.rideservice.event.RideRequestedEvent;
import com.rideshare.rideservice.model.Ride;
import com.rideshare.rideservice.model.RideStatus;
import com.rideshare.rideservice.repository.RideRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class RideService {
    private final RideRepository rideRepository;
    private final KafkaTemplate<String, RideRequestedEvent> kafkaTemplate;

    private static final String RIDE_REQUESTED_TOPIC = "ride.requested";


    /**
     * Create ride in DB with REQUESTED STATUS
     */
    public RideResponse requestRide(RideRequest request){
        log.info("New ride request from rider: {}", request.getRiderId());

        // Step 1: save ride to database
        Ride ride = new Ride();
        ride.setRiderId(request.getRiderId());
        ride.setPickupLatitude(request.getPickupLatitude());
        ride.setPickupLongitude(request.getPickupLongitude());
        ride.setPickupAddress(request.getPickupAddress());
        ride.setDropLatitude(request.getDropLatitude());
        ride.setDropLongitude(request.getDropLongitude());
        ride.setDropAddress(request.getDropAddress());
        ride.setStatus(RideStatus.REQUESTED);
        ride.setEstimatedFare(calculateEstimateFare(request));

        Ride savedRide = rideRepository.save(ride);

        // Step 2: Publish event to kafka
        // Matching service will consume this and find the nearest driver
        RideRequestedEvent rideRequestedEvent = new RideRequestedEvent(
                savedRide.getId(),
                savedRide.getRiderId(),
                savedRide.getPickupLatitude(),
                savedRide.getPickupLongitude(),
                savedRide.getPickupAddress(),
                savedRide.getDropLatitude(),
                savedRide.getDropLongitude(),
                savedRide.getDropAddress()
        );

        kafkaTemplate.send(RIDE_REQUESTED_TOPIC, savedRide.getId(), rideRequestedEvent);
        log.info("RideRequestedEvent published to Kafka for ride: {}", savedRide.getRiderId());

        // Update status to Matching
        savedRide.setStatus(RideStatus.MATCHING);
        rideRepository.save(savedRide);

        return mapToResponse(savedRide);
    }


    /**
     *
     * @param rideId
     * @param driverId
     */
    public void updateRideWithDriver(String rideId, String driverId){
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new RuntimeException(
                        "Ride with id: " + rideId + " not found."
                ));

        ride.setDriverId(driverId);
        ride.setStatus(RideStatus.ACCEPTED);
        rideRepository.save(ride);
    }


    /**
     *
     * @param rideId
     * @return
     */
    public RideResponse startRide(String rideId){
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new RuntimeException("Ride with id: " + rideId + " not found."));

        if(ride.getStatus() != RideStatus.ACCEPTED){
            throw new RuntimeException("Ride cannot be started. Current status: " + ride.getStatus());
        }

        ride.setStatus(RideStatus.RIDE_STARTED);
        ride.setStartedAt(LocalDateTime.now());
        rideRepository.save(ride);

        return mapToResponse(ride);
    }


    /**
     *
     * @param rideId
     * @return
     */
    public RideResponse completeRide(String rideId){
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new RuntimeException("Ride with id: " + rideId + " not found."));

        if(ride.getStatus() != RideStatus.RIDE_STARTED){
            throw new RuntimeException("Ride cannot be started. Current status: " + ride.getStatus());
        }

        ride.setStatus(RideStatus.COMPLETED);
        ride.setCompletedAt(LocalDateTime.now());
        ride.setActualFare(ride.getEstimatedFare());
        rideRepository.save(ride);

        return mapToResponse(ride);
    }

    /**
     *
     * @param rideId
     * @return
     */
    public RideResponse cancelRide(String rideId){
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new RuntimeException("Ride with id: " + rideId + " not found."));

        ride.setStatus(RideStatus.CANCELLED);
        rideRepository.save(ride);

        return mapToResponse(ride);
    }


    public RideResponse getRideById(String rideId){
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new RuntimeException("Ride with id: " + rideId + " not found."));

        return mapToResponse(ride);
    }

    public List<RideResponse> getRidesByRider(String  riderId){
        return rideRepository.findByRiderIdOrderByCreatedAtDesc(riderId)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    private double calculateEstimateFare(RideRequest request){
        double lat1 = Math.toRadians(request.getPickupLatitude());
        double lon1 = Math.toRadians(request.getPickupLongitude());
        double lat2 = Math.toRadians(request.getDropLatitude());
        double lon2 = Math.toRadians(request.getDropLongitude());

        double dLat = lat2 - lat1;
        double dLon = lon2 - lon1;

        double a = Math.pow(Math.sin(dLat / 2), 2)
                + Math.cos(lat1) * Math.cos(lat2)
                * Math.pow(Math.sin(dLon / 2), 2);

        double c = 2 * Math.asin(Math.sqrt(a));
        double distanceKm = 6371 * c;

        // Base fare: 50Rs + 12Rs perKm
        double fare = 50 + distanceKm*12;
        return Math.round(fare*100.0)/100.0;
    }

    /**
     *
     * @param savedRide
     * @return
     */
    private RideResponse mapToResponse(Ride savedRide){
        RideResponse rideResponse = new RideResponse();
        rideResponse.setId(savedRide.getId());
        rideResponse.setRiderId(savedRide.getRiderId());
        rideResponse.setDriverId(savedRide.getDriverId());

        rideResponse.setPickupAddress(savedRide.getPickupAddress());
        rideResponse.setPickupLatitude(savedRide.getPickupLatitude());
        rideResponse.setPickupLongitude(savedRide.getPickupLongitude());
        rideResponse.setDropAddress(savedRide.getDropAddress());
        rideResponse.setDropLatitude(savedRide.getDropLatitude());
        rideResponse.setDropLongitude(savedRide.getDropLongitude());

        rideResponse.setStatus(savedRide.getStatus());
        rideResponse.setEstimatedFare(savedRide.getEstimatedFare());
        rideResponse.setActualFare(savedRide.getActualFare());

        rideResponse.setCreatedAt(savedRide.getCreatedAt());
        rideResponse.setStartedAt(savedRide.getStartedAt());
        rideResponse.setCompletedAt(savedRide.getCompletedAt());
        return rideResponse;
    }

}
