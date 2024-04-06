import Vue from 'vue'
import Router from 'vue-router'
import LandingPage from '../views/LandingPage.vue'
import Dashboard from '../views/Dashboard.vue'
import GuestDashboard from '../views/GuestDashboard.vue'
import Login from '../views/Login.vue'
import Logout from '../views/Logout.vue'
import Register from '../views/Register.vue'
import store from '../store/index'
import About from '../views/About.vue'
import PrivacyPolicy from '../views/PrivacyPolicy.vue'
import TermsOfService from '../views/TermsOfService.vue'
import { bus } from '../event-bus.js';

Vue.use(Router)

const router = new Router({
  mode: 'history',
  base: process.env.BASE_URL,
  routes: [
    {
      path: '/',
      name: 'landing',
      component: LandingPage,
      meta: {
        requiresAuth: false
      }
    },
    {
      path: '/dashboard',
      name: 'dashboard',
      component: Dashboard,
      meta: {
        requiresAuth: true
      },
    },
    {
      path: '/guest-dashboard',
      name: 'guest-dashboard',
      component: GuestDashboard,
    },
    {
      path: "/login",
      name: "login",
      component: Login,
      meta: {
        requiresAuth: false
      }
    },
    {
      path: "/logout",
      name: "logout",
      component: Logout,
      meta: {
        requiresAuth: false
      }
    },
    {
      path: "/register",
      name: "register",
      component: Register,
      meta: {
        requiresAuth: false
      }
    },
    {
      path: "/about",
      name: "about",
      component: About,
      meta: {
        requiresAuth: false
      }
    },
    {
      path: "/privacy-policy",
      name: "privacy-policy",
      component: PrivacyPolicy,
      meta: {
        requiresAuth: false
      }
    },
    {
      path: "/terms-of-service",
      name: "terms-of-service",
      component: TermsOfService,
      meta: {
        requiresAuth: false
      }
    }
  ],
  scrollBehavior() {
    return { x: 0, y: 0 };
  }
});

router.beforeEach((to, from, next) => {
  // Determine if the route requires authentication
  const requiresAuth = to.matched.some(x => x.meta.requiresAuth);

  // Determine if the user is logged in
  const isAuthenticated = store.state.token !== '';

  // If the route requires authentication and the user is not logged in, redirect to login
  if (requiresAuth && !isAuthenticated) {
    next("/login");

  // If the user is logged in, redirect to dashboard
  } else if  (to.path === '/' && isAuthenticated) {
    next("/dashboard");

  // Else let them go to their next destination
  } else {
    next();
  }
});

router.afterEach(() => {
  bus.$emit('reset-notification-modal');
});

export default router;
