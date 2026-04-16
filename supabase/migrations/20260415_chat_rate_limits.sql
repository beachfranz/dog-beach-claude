-- ============================================================
-- Chat rate limiting table + helper function
-- Tracks requests per IP per hour for beach-chat.
-- Edge function uses service role and bypasses RLS.
-- No anon access — table is fully locked.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.chat_rate_limits (
  ip      text        NOT NULL,
  hour    timestamptz NOT NULL,   -- truncated to the hour
  count   integer     NOT NULL DEFAULT 1,
  PRIMARY KEY (ip, hour)
);

ALTER TABLE public.chat_rate_limits ENABLE ROW LEVEL SECURITY;
-- No policies → anon access is fully blocked.

-- Atomically increment the counter for (ip, hour) and return the new count.
CREATE OR REPLACE FUNCTION public.increment_chat_rate(p_ip text, p_hour timestamptz)
RETURNS integer
LANGUAGE sql
SECURITY DEFINER
AS $$
  INSERT INTO public.chat_rate_limits (ip, hour, count)
  VALUES (p_ip, p_hour, 1)
  ON CONFLICT (ip, hour)
  DO UPDATE SET count = chat_rate_limits.count + 1
  RETURNING count;
$$;
