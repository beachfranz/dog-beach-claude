-- Time window during which dogs are prohibited (e.g. Corona del Mar 10:00–16:30)
-- Stored as "HH:MM" strings, same format as open_time / close_time.
-- NULL on both columns means no prohibition applies.
ALTER TABLE public.beaches
  ADD COLUMN IF NOT EXISTS dogs_prohibited_start text,
  ADD COLUMN IF NOT EXISTS dogs_prohibited_end   text;
