// config.js - Configuration loader for Supabase credentials
// This file reads from a separate config that's gitignored

// Load configuration from window object (set by config-local.js)
const SUPABASE_CONFIG = window.SUPABASE_CONFIG || {
    url: null,
    anonKey: null
};

// Export for use in other scripts
const SUPABASE_URL = SUPABASE_CONFIG.url;
const SUPABASE_ANON_KEY = SUPABASE_CONFIG.anonKey;

// Validate credentials are present
if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    console.error('❌ Supabase credentials not configured!');
    console.error('Create config-local.js from config-local.example.js');
}

// Initialize Supabase client
const { createClient } = supabase;
const supabaseClient = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

console.log('✅ Supabase client initialized');
console.log('📡 Connecting to:', SUPABASE_URL);