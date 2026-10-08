package com.heatherpiper.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.heatherpiper.dao.GameDao;
import com.heatherpiper.dao.TeamDao;
import com.heatherpiper.model.Game;
import io.netty.handler.timeout.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Year;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service class for handling operations related to the Squiggle API.
 */
@Service
public class SquiggleService {

    private static final Logger logger = LoggerFactory.getLogger(SquiggleService.class);
    private static final String USER_AGENT = "Later Ladder (github.com/heatherpiper/Later-Ladder; heather.a.piper@gmail.com)";

    private static final long MIN_REFRESH_INTERVAL = 5 * 60 * 1000; // 5 minutes
    private long lastRefreshTime = 0;

    private Disposable gameUpdateSubscription;

    private final HttpClient httpClient;
    private final GameDao gameDao;
    private final TeamDao teamDao;
    private final ObjectMapper objectMapper;

    /**
     * Constructor for the SquiggleService class.
     *
     * @param httpClient   The HTTP client to be used for making requests.
     * @param gameDao      The DAO for accessing game data.
     * @param teamDao      The DAO for accessing team data.
     * @param objectMapper The object mapper for JSON serialization/deserialization.
     */
    @Autowired
    public SquiggleService(HttpClient httpClient, GameDao gameDao, TeamDao teamDao, ObjectMapper objectMapper) {
        this.httpClient = httpClient;
        this.gameDao = gameDao;
        this.teamDao = teamDao;
        this.objectMapper = objectMapper;
    }

    /**
     * This method is annotated with @PostConstruct, which means it is automatically
     * called by the Spring framework after the SquiggleService bean is instantiated and
     * dependency injection is complete. In this method, we call the subscribeToGameUpdates()
     * method to start the subscription to game updates from the Squiggle API. This ensures
     * that as soon as the SquiggleService is ready, it starts receiving game updates.
     */
    @PostConstruct
    public void init() {
        subscribeToGameUpdates();
    }

    /**
     * Fetches games for a specific year and round from the Squiggle API.
     *
     * <p>This method constructs a URL to the Squiggle API's games endpoint, specifying the year and round as query parameters.
     * It then sends a GET request to this URL and parses the response body into a list of Game objects.
     *
     * <p>If the list of games is not empty, the games are saved to the database.
     *
     * <p>If an exception occurs during the request or parsing process, an error message is logged and an empty list is returned.
     *
     * @param year The year for which to fetch games.
     * @param round The round for which to fetch games.
     * @return A list of games for the specified year and round. If an error occurs, an empty list is returned.
     */
    public List<Game> fetchGamesForYearAndRound(int year, int round) {
        String url = "https://api.squiggle.com.au/?q=games;year=" + year + ";round=" + round;
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .header("User-Agent", USER_AGENT)
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            List<Game> games = parseGames(response.body());

            if (!games.isEmpty()) {
                gameDao.saveAll(games);
            }
            return games;

        } catch (Exception e) {
            logger.error("Failed to fetch games for year {} and round {}: ", year, round, e);
            return List.of();
        }
    }

    /**
     * Fetches games for a specific year from the Squiggle API.
     *
     * <p>This method constructs a URL to the Squiggle API's games endpoint, specifying the year as a query parameter.
     * It then sends a GET request to this URL and parses the response body into a list of Game objects.
     *
     * <p>If the list of games is not empty, the games are saved to the database.
     *
     * <p>If an exception occurs during the request or parsing process, an error message is logged and an empty list is returned.
     *
     * @param year The year for which to fetch games.
     * @return A list of games for the specified year. If an error occurs, an empty list is returned.
     */
    public List<Game> fetchGamesForYear(int year) {
        String url = "https://api.squiggle.com.au/?q=games;year=" + year;
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .header("User-Agent", USER_AGENT)
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            List<Game> games = parseGames(response.body());

            if (!games.isEmpty()) {
                gameDao.saveAll(games);
            }
            return games;

        } catch (Exception e) {
            logger.error("Failed to fetch games for year {}: ", year, e);
            return List.of();
        }
    }

    /**
     * Fetches games from the most recent round for a specific year from the Squiggle API.
     *
     * <p>This method sends an HTTP request to the Squiggle API endpoint with the appropriate headers and query parameters to retrieve the games for the specified year. The response body is then parsed to obtain a list of Game objects.
     *
     * <p>The method determines the highest completed round by filtering the games based on the 'complete' property (which indicates the
     * percentage of completion) and finding the maximum round number. If a valid highest completed round is found (i.e., not equal to -1), the method fetches the games for that round using the 'fetchGamesForYearAndRound' method and adds them to the 'gamesFromMostRecentRound' list.
     *
     * <p>If an exception occurs during the process, an error message is logged.
     *
     * @param year The year for which to fetch games.
     * @return A list of games from the most recent round for the specified year. If an error occurs, an empty list is returned.
     */
    public List<Game> fetchGamesFromMostRecentRound(int year) {
        List<Game> gamesFromMostRecentRound = new ArrayList<>();

        try {
            String url = String.format("https://api.squiggle.com.au/?q=games;year=%d", year);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Accept", "application/json")
                    .header("User-Agent", USER_AGENT)
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            String responseBody = response.body();

            List<Game> games = parseGames(responseBody);

            // Determine the highest completed round
            int highestCompletedRound = games.stream()
                    .filter(game -> game.getComplete() == 100)
                    .mapToInt(Game::getRound)
                    .max()
                    .orElse(-1);

            if (highestCompletedRound != -1) {
                gamesFromMostRecentRound.addAll(fetchGamesForYearAndRound(year, highestCompletedRound));
            }
        } catch (Exception e) {
            logger.error("Failed to fetch games from most recent round for year {}: ", year, e);
        }
        return gamesFromMostRecentRound;
    }

    /**
     * Fetches games up to the most recent round for a specific year from the Squiggle API.
     *
     * <p>This method constructs a URL to the Squiggle API's games endpoint, specifying the year as a query parameter.
     * It then sends a GET request to this URL and parses the response body into a list of Game objects.
     *
     * <p>The method determines the highest completed round for the year. If no games are completed and it's not the current year or later,
     * the method recursively calls itself with the previous year as the parameter.
     *
     * <p>If games are completed, it fetches games for all rounds up to the highest completed round and adds them to the list of all games.
     *
     * <p>If an exception occurs during the request or parsing process, an error message is logged.
     *
     * @param year The year for which to fetch games.
     * @return A list of games for the specified year up to the most recent round. If an error occurs, an empty list is returned.
     */
    public List<Game> fetchGamesUpToMostRecentRound(int year) {
        List<Game> allGames = new ArrayList<>();

        try {
            String url = String.format("https://api.squiggle.com.au/?q=games;year=%d", year);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Accept", "application/json")
                    .header("User-Agent", USER_AGENT)
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            String responseBody = response.body();

            List<Game> games = parseGames(responseBody);

            // Determine the highest completed round
            int highestCompletedRound = games.stream()
                    .filter(game -> game.getComplete() == 100)
                    .mapToInt(Game::getRound)
                    .max()
                    .orElse(-1);

            boolean isCurrentYearOrLater = year >= LocalDate.now().getYear();
            boolean shouldFallbackToPreviousYear = highestCompletedRound == -1 && !isCurrentYearOrLater;

            if (shouldFallbackToPreviousYear) {
                // If no games are completed, and it's not the current year or later, try the previous year
                return fetchGamesUpToMostRecentRound(year - 1);
            } else {
                for (int round = 0; round <= highestCompletedRound; round++) {
                    allGames.addAll(fetchGamesForYearAndRound(year, round));
                }
            }
        } catch (Exception e) {
            logger.error("Failed to fetch games up to most recent round for year {}: ", year, e);
        }
        return allGames;
    }

    /**
     * Initiates a refresh of game data for a specific year. This method is typically called by an admin as a backup method in case
     * automatic game updates via SSEs fail to update the database as expected.
     *
     * <p>The method first validates the year. It must be within the last 2 years. If the year is invalid, an IllegalArgumentException is thrown.
     *
     * <p>Next, the method checks if a refresh operation is allowed. Refresh operations are rate-limited to prevent excessive requests.
     * If the last refresh was less than 5 minutes ago, an IllegalStateException is thrown.
     *
     * <p>If the year is valid and a refresh operation is allowed, the method fetches games up to the most recent round for the specified year.
     * If the fetch operation is successful, a success message is logged. If an error occurs during the fetch operation, an error message is logged
     * and a RuntimeException is thrown.
     *
     * @param year The year for which to refresh game data.
     * @throws IllegalArgumentException If the year is not within the last 2 years.
     * @throws IllegalStateException If a refresh operation is not allowed because the last refresh was less than 5 minutes ago.
     * @throws RuntimeException If an error occurs during the fetch operation.
     */
    public void adminInitiatedRefresh(int year) {
        logger.info("Admin initiated refresh of games data for current year");

        // Validate year
        if (year < Year.now().getValue() - 2 || year > Year.now().getValue()) {
            logger.warn("Invalid year provided for refresh: {}", year);
            throw new IllegalArgumentException("Invalid year for refresh. Year must be within the last 2 years.");
        }

        // Rate limiting check
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastRefreshTime < MIN_REFRESH_INTERVAL) {
            logger.warn("Refresh operation is rate-limited. Last refresh was less than 5 minutes ago.");
            throw new IllegalStateException("Refresh operation is rate-limited. Please wait before trying again.");
        }
        lastRefreshTime = currentTime;

        try {
            fetchGamesUpToMostRecentRound(year);
            logger.info("Successfully refreshed game data for year {}", year);
        } catch (Exception e) {
            logger.error("Error during admin-initiated refresh of games data for year {}: ", year, e);
            throw new RuntimeException("Failed to refresh game data due to an error. Please check the logs for more details.");
        }
    }

    /**
     * Parses a JSON response body from the Squiggle API into a list of Game objects.
     *
     * <p>This method uses Jackson's ObjectMapper to parse the JSON response body. It first reads the response body into a JsonNode tree,
     * then extracts the "games" array from the root node. It iterates over each element in the "games" array, deserializes it into a Game object,
     * and adds it to a list of games.
     *
     * <p>If the "games" field in the response body is not an array or if an error occurs during the parsing process, an error message is logged
     * and an empty list is returned.
     *
     * @param responseBody The JSON response body from the Squiggle API.
     * @return A list of Game objects parsed from the response body. If an error occurs, an empty list is returned.
     * @throws IOException If an error occurs during the parsing process.
     */
    private List<Game> parseGames(String responseBody) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Game> gamesList = new ArrayList<>();
        try {
            JsonNode rootNode = objectMapper.readTree(responseBody);
            JsonNode gamesNode = rootNode.path("games");
            if (gamesNode.isArray()) {
                for (JsonNode gameNode : gamesNode) {
                    Game game = objectMapper.treeToValue(gameNode, Game.class);
                    gamesList.add(game);
                }
            }
            return gamesList;
        } catch (IOException e) {
            logger.error("Failed to parse games from response body.", e);
            return List.of();
        }
    }

    /**
     * Subscribes to game updates from the Squiggle API's Server-Sent Events (SSE) endpoint.
     *
     * <p>If a previous subscription exists and is not disposed, it is disposed and a log message is printed.
     *
     * <p>The method then attempts to connect to the Squiggle SSE endpoint. It creates a Flux that emits items representing
     * Server-Sent Events from the endpoint. The Flux is configured to retry on IOException or TimeoutException, with a backoff
     * strategy that starts with a delay of 3 seconds and increases exponentially for each subsequent retry, up to a maximum of 1 minute.
     *
     * <p>Upon subscribing to the Flux, a log message is printed. The Flux is then subscribed to, with the following behaviors defined:
     * - On each emitted item, the processSseEvent method is called to process the event.
     * - On error, a log message prints and the method recursively calls itself to attempt to reconnect.
     * - On completion, a log message prints and the method recursively calls itself to attempt to reconnect.
     */
    public void subscribeToGameUpdates() {
        if (gameUpdateSubscription != null && !gameUpdateSubscription.isDisposed()) {
            gameUpdateSubscription.dispose();
            logger.info("Disposed the previous game update subscription.");
        }
        logger.info("Attempting to connect to the Squiggle SSE endpoint...");

        Flux<String> eventStream = reactor.netty.http.client.HttpClient.create()
                .headers(h -> h.set("User-Agent", USER_AGENT))
                .get()
                .uri("https://sse.squiggle.com.au/games")
                .responseContent()
                .asString()
                .windowUntil(s -> s.contains("\n\n"))
                .flatMap(w -> w.reduce(String::concat));

        Predicate<Throwable> isRetryableException = throwable ->
                throwable instanceof IOException;

        gameUpdateSubscription = eventStream
                .doOnSubscribe(subscription -> logger.info("Subscribed to Game Event Stream"))
                .retryWhen(Retry.backoff(10, Duration.ofSeconds(3))
                        .filter(throwable -> throwable instanceof IOException || throwable instanceof TimeoutException)
                        .doBeforeRetry(retrySignal -> logger.info("Reconnecting attempt #{}, due to: {}", retrySignal.totalRetries() + 1,
                                retrySignal.failure().getMessage()))
                        .maxBackoff(Duration.ofMinutes(1)))
                .subscribe(
                        this::processSseEvent,
                        error -> {
                            logger.error("Error on Game Event Stream", error);
                            subscribeToGameUpdates();
                        },
                        () -> {
                            logger.info("Game Event Stream completed");
                            subscribeToGameUpdates();
                        }
                );
    }

    /**
     * Processes a Server-Sent Event (SSE) from the Squiggle API's game updates endpoint.
     *
     * <p>This method is triggered upon receiving an event from the SSE endpoint. It checks if the event starts with "event:"
     * and then extracts the event type. If the event type is "removeGame", it means the game is over and the final scores are available.
     *
     * <p>The method then extracts the data from the event, deserializes it into a Game object (converting team IDs into team names for
     * hteam, ateam, and winner), and saves it to the database.
     *
     * @param sseEvent The Server-Sent Event received from the Squiggle API's game updates endpoint.
     */
    private void processSseEvent(String sseEvent) {
        String trimmedEvent = sseEvent.trim();
        if (trimmedEvent.equals(":") || trimmedEvent.isEmpty()) {
            logger.debug("Heartbeat received to keep the connection alive.");
            return;
        }

        try {
            String eventType = extractEventType(trimmedEvent);
            if ("removeGame".equals(eventType)) {
                String data = extractData(trimmedEvent);
                Game game = objectMapper.readValue(data, Game.class);

                try {
                    game.setHteam(teamDao.findTeamNameById(Integer.parseInt(game.getHteam())));
                } catch (NumberFormatException e) {
                    logger.warn("hteam is not an ID, assuming it's a name for Game ID {}", game.getId());
                }

                try {
                    game.setAteam(teamDao.findTeamNameById(Integer.parseInt(game.getAteam())));
                } catch (NumberFormatException e) {
                    logger.warn("ateam is not an ID, assuming it's a name for Game ID {}", game.getId());
                }

                try {
                    game.setWinner(teamDao.findTeamNameById(Integer.parseInt(game.getWinner())));
                } catch (NumberFormatException e) {
                    logger.warn("winner is not an ID, assuming it's a name for Game ID {}", game.getId());
                }

                gameDao.saveAll(Collections.singletonList(game));
                logger.info("Processed 'removeGame' event for Game ID: {}", game.getId());
            } else {
                logger.debug("Received an unhandled event type: {}", eventType);
            }
        } catch (JsonProcessingException e) {
            logger.error("Error processing SSE event: {}", trimmedEvent, e);
        }
    }

    /**
     * Extracts the event type from a Server-Sent Event (SSE) string.
     *
     * <p>This method uses a regular expression to match the "event:" field in the SSE string, which represents the event type.
     * The regular expression is case-insensitive and works across multiple lines.
     *
     * <p>If the "event:" field is found, the method returns the event type as a trimmed string. If the "event:" field is not found,
     * the method returns the string "unknown".
     *
     * @param sseEvent The Server-Sent Event string from which to extract the event type.
     * @return The event type extracted from the SSE string, or "unknown" if the "event:" field is not found.
     */
    private String extractEventType(String sseEvent) {
        Matcher matcher = Pattern.compile("event:\\s*(\\w+)", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE).matcher(sseEvent);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return "unknown";
    }

    /**
     * Extracts the data field from a Server-Sent Event (SSE) string.
     *
     * <p>This method uses a regular expression to match the "data:" field in the SSE string, which represents the event data.
     * The regular expression is designed to capture a JSON object, and works across multiple lines due to the DOTALL flag.
     *
     * <p>If the "data:" field is found, the method returns the data as a trimmed string. If the "data:" field is not found,
     * the method returns an empty JSON object string "{}".
     *
     * @param sseEvent The Server-Sent Event string from which to extract the data.
     * @return The data extracted from the SSE string, or an empty JSON object string "{}" if the "data:" field is not found.
     */
    private String extractData(String sseEvent) {
        Matcher matcher = Pattern.compile("data:\\s*(\\{.*?})", Pattern.DOTALL).matcher(sseEvent);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return "{}";
    }

    /**
     * This method is annotated with @PreDestroy, which means it is automatically
     * called by the Spring framework just before the SquiggleService bean is destroyed.
     * This method is used to clean up any resources that the bean has been using.
     *
     * <p>Specifically, this method checks if the gameUpdateSubscription exists and is not disposed.
     * If so, it disposes the subscription. This is important to prevent memory leaks and other
     * potential issues related to the lifecycle of the subscription.
     */
    @PreDestroy
    public void onDestroy() {
        if (gameUpdateSubscription != null && !gameUpdateSubscription.isDisposed()) {
            gameUpdateSubscription.dispose();
        }
    }
}
