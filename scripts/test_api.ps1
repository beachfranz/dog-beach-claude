$r = Invoke-WebRequest -Uri 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/get-beach-detail?location_id=huntington-dog-beach&date=2026-04-18' -Headers @{'Authorization'='Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk'} -UseBasicParsing
$j = $r.Content | ConvertFrom-Json
$h = $j.hours[0]
Write-Host "weather_code:  $($h.weather_code)"
Write-Host "weather_score: $($h.weather_score)"
Write-Host "tide_score:    $($h.tide_score)"
Write-Host "uv_score:      $($h.uv_score)"
Write-Host "hour_score:    $($h.hour_score)"
