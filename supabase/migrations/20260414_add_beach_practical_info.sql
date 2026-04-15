-- Add practical info columns to beaches table
ALTER TABLE public.beaches
  ADD COLUMN IF NOT EXISTS leash_policy text,
  ADD COLUMN IF NOT EXISTS dog_rules text,
  ADD COLUMN IF NOT EXISTS amenities text,
  ADD COLUMN IF NOT EXISTS restrooms text;
