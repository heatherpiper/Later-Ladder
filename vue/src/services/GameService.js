import axios from 'axios';

const API_URL = process.env.VUE_APP_REMOTE_API

export default {
    getAllGames() {
        return axios.get(`${API_URL}/games`);
    },

    getGameById(gameId) {
        return axios.get(`${API_URL}/games/${gameId}`);
    },

    getGamesByRound(round) {
        return axios.get(`${API_URL}/round/${round}`);
    },

    getCompleteGames() {
        return axios.get(`${API_URL}/complete`);
    },

    getIncompleteGames() {
        return axios.get(`${API_URL}/incomplete`);
    }
}