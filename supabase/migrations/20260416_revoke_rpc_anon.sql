-- ============================================================
-- Revoke anon execute on increment_chat_rate
-- The function is only needed by the beach-chat edge function
-- which runs as service role. Anon should never call it directly.
-- ============================================================

REVOKE EXECUTE ON FUNCTION public.increment_chat_rate(text, timestamptz) FROM anon;
