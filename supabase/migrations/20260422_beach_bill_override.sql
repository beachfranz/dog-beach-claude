-- Add beach_bill_override flag to state_config.
--
-- Why: v2-blm-sma-rescue uses the BLM Surface Management Agency service to
-- detect federal and private lands the primary polygon classifiers missed.
-- For CA this works cleanly. For Oregon it's dangerous: Beach Bill (1967)
-- establishes the entire Ocean Shore as state-managed public land regardless
-- of underlying parcel ownership. BLM's SMA layer reports the parcel owner,
-- so ~55 Ocean Shore beaches in OR have adjacent parcels tagged 'private'.
-- Without this flag, the rescue would mark them review_status='invalid',
-- wiping out legitimate public beaches.
--
-- When beach_bill_override = true, v2-blm-sma-rescue still performs the
-- federal-rescue branch (catches USACE jetties, NFS boundaries, etc.) but
-- skips the private-land invalidation branch entirely.

alter table public.state_config
  add column if not exists beach_bill_override boolean not null default false;

comment on column public.state_config.beach_bill_override is
  'When true, v2-blm-sma-rescue skips private-land invalidation for this state. Use for states with Beach Bill-style public-beach statutes (OR, TX).';

update public.state_config
  set beach_bill_override = true
  where state_code = 'OR';
