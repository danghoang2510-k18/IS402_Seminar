-- Azure Stream Analytics / iotAnalytic
-- Input : Input_IoTHub
-- Output: Output_Storage_Trends, Output_Storage_Alerts
-- Calibrated exploratory thresholds from FreeManufacturingQC100M profiling.
-- These are dataset-specific DEMO bounds, not industrial safety limits.
-- KEEP both output aliases configured in Azure Stream Analytics.
-- Simulator sends SensorValue as JSON number, unit as case-sensitive source text,
-- SourceAnomalyFlag as integer (0 or 1), and UTC ISO-8601 EventTimestamp.
-- Alerts use a 3-minute SlidingWindow and may produce repeated states.

WITH InputEvents AS (
    SELECT
        MachineId,
        ProductionLine,
        LOWER(SensorType) AS SensorType,
        [unit] AS SensorUnit,
        TRY_CAST(SensorValue AS float) AS SensorValue,
        CASE WHEN SourceAnomalyFlag = 1 THEN 1 ELSE 0 END AS SourceAnomalyFlag
    FROM [Input_IoTHub] TIMESTAMP BY EventTimestamp
    WHERE MachineId IS NOT NULL
      AND SensorType IS NOT NULL
      AND [unit] IS NOT NULL
),

Thresholded AS (
    SELECT
        MachineId,
        ProductionLine,
        SensorType,
        SensorUnit,
        SensorValue,
        SourceAnomalyFlag,
        CASE
            WHEN SensorType = 'temperature'  AND SensorUnit = 'F'    THEN 70.0
            WHEN SensorType = 'vibration'    AND SensorUnit = 'mm/s' THEN 0.46
            WHEN SensorType = 'pressure'     AND SensorUnit = 'psi'  THEN 93.0
            WHEN SensorType = 'current_draw' AND SensorUnit = 'A'    THEN 13.85
            WHEN SensorType = 'humidity'     AND SensorUnit = '%'    THEN 41.5
            ELSE NULL
        END AS LowerThreshold,
        CASE
            WHEN SensorType = 'temperature'  AND SensorUnit = 'F'    THEN 80.0
            WHEN SensorType = 'vibration'    AND SensorUnit = 'mm/s' THEN 0.54
            WHEN SensorType = 'pressure'     AND SensorUnit = 'psi'  THEN 107.0
            WHEN SensorType = 'current_draw' AND SensorUnit = 'A'    THEN 16.15
            WHEN SensorType = 'humidity'     AND SensorUnit = '%'    THEN 48.5
            ELSE NULL
        END AS UpperThreshold
    FROM InputEvents
),

Flagged AS (
    SELECT
        MachineId,
        ProductionLine,
        SensorType,
        SensorUnit,
        SensorValue,
        LowerThreshold,
        UpperThreshold,
        SourceAnomalyFlag,
        CASE WHEN SensorValue < LowerThreshold THEN 1 ELSE 0 END AS LowFlag,
        CASE WHEN SensorValue > UpperThreshold THEN 1 ELSE 0 END AS HighFlag
    FROM Thresholded
    WHERE SensorValue IS NOT NULL
      AND LowerThreshold IS NOT NULL
      AND UpperThreshold IS NOT NULL
),

RiskAnalysis AS (
    SELECT
        MachineId,
        ProductionLine,
        SensorType,
        SensorUnit,
        System.Timestamp() AS WindowEndTime,
        COUNT(*) AS ReadingCount,
        MIN(SensorValue) AS MinValue,
        AVG(SensorValue) AS AvgValue,
        MAX(SensorValue) AS MaxValue,
        MIN(LowerThreshold) AS LowerThreshold,
        MAX(UpperThreshold) AS UpperThreshold,
        SUM(LowFlag) AS LowCount,
        SUM(HighFlag) AS HighCount,
        SUM(SourceAnomalyFlag) AS SourceAnomalyCount
    FROM Flagged
    GROUP BY
        MachineId,
        ProductionLine,
        SensorType,
        SensorUnit,
        SlidingWindow(minute, 3)
)

-- OUTPUT 1: Non-overlapping five-minute sensor trends.
SELECT
    MachineId,
    ProductionLine,
    SensorType,
    SensorUnit,
    DATEADD(minute, -5, System.Timestamp()) AS WindowStartTime,
    System.Timestamp() AS WindowEndTime,
    COUNT(*) AS ReadingCount,
    MIN(SensorValue) AS MinValue,
    AVG(SensorValue) AS AvgValue,
    MAX(SensorValue) AS MaxValue,
    MIN(LowerThreshold) AS LowerThreshold,
    MAX(UpperThreshold) AS UpperThreshold,
    SUM(LowFlag) AS LowCount,
    SUM(HighFlag) AS HighCount,
    SUM(SourceAnomalyFlag) AS SourceAnomalyCount
INTO [Output_Storage_Trends]
FROM Flagged
GROUP BY
    MachineId,
    ProductionLine,
    SensorType,
    SensorUnit,
    TumblingWindow(minute, 5);

-- OUTPUT 2: 3-minute sliding-window alerts.
-- At least 3 readings, 2 out-of-range readings, >=20% violating:
-- reduces alerts from isolated measurement spikes (does not deduplicate windows).
-- Severity (demo heuristic): >=50% violating AND >=3 violations = Critical;
-- otherwise Warning. Do not interpret as validated industrial thresholds.
SELECT
    MachineId,
    ProductionLine,
    SensorType,
    SensorUnit,
    DATEADD(minute, -3, WindowEndTime) AS WindowStartTime,
    WindowEndTime AS AlertTriggerTime,
    ReadingCount,
    MinValue,
    AvgValue,
    MaxValue,
    LowerThreshold,
    UpperThreshold,
    LowCount,
    HighCount,
    (LowCount + HighCount) AS OutOfRangeCount,
    SourceAnomalyCount,
    CASE
        WHEN LowCount > 0 AND HighCount > 0 THEN 'Both'
        WHEN LowCount > 0 THEN 'Too Low'
        WHEN HighCount > 0 THEN 'Too High'
        ELSE 'None'
    END AS AlertDirection,
    CASE
        WHEN (LowCount + HighCount) >= 3
             AND (LowCount + HighCount) * 1.0 >= ReadingCount * 0.50
            THEN 'Critical'
        ELSE 'Warning'
    END AS Severity,
    CASE
        WHEN SensorType = 'temperature'  THEN 'Temperature out of demo range'
        WHEN SensorType = 'vibration'    THEN 'Vibration out of demo range'
        WHEN SensorType = 'pressure'     THEN 'Pressure out of demo range'
        WHEN SensorType = 'current_draw' THEN 'Current draw out of demo range'
        WHEN SensorType = 'humidity'     THEN 'Humidity out of demo range'
        ELSE 'Sensor out of demo range'
    END AS ErrorDescription
INTO [Output_Storage_Alerts]
FROM RiskAnalysis
WHERE ReadingCount >= 3
  AND (LowCount + HighCount) >= 2
  AND (LowCount + HighCount) * 1.0 >= ReadingCount * 0.20;
