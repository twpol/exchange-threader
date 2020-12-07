$lastSubject = ''
$seenSent = @{}
foreach ($line in Get-Content '.\Changes.txt') {
    if ($line -match '^[^ ]') {
        $lastSubject = $line
    } elseif ($line -match '^  [0-9]') {
        $part = $line.Trim() -split ' '
        $sentDate = ($part[0], $part[1]) -join ' '
        $receiveDate = ($part[3], $part[4]) -join ' '
        $key = "$lastSubject <-> $sentDate"
        if (-not $seenSent.$key) {
            $seenSent.$key = $receiveDate
            Write-Output "$key --> $receiveDate"
        }
    }
}
