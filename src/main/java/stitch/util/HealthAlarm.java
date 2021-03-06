package stitch.util;

import java.io.Serializable;

public class HealthAlarm implements Serializable {

    private static final long serialVersionUID = 6894L;

    private String subjectId;
    private AlarmSeverity severity;
    private String alarmName;
    private String alarmDescription;

    public HealthAlarm(String subjectId, AlarmSeverity severity, String alarmName, String alarmDescription) {
        this.subjectId = subjectId;
        this.severity = severity;
        this.alarmName = alarmName;
        this.alarmDescription = alarmDescription;
    }

    public String getSubjectId() { return subjectId; }

    public AlarmSeverity getSeverity() { return severity; }

    public String getAlarmName() { return alarmName; }

    public String getAlarmDescription() { return alarmDescription; }

    public enum AlarmSeverity {
        INFO,
        WARN,
        ERROR,
        CRITICAL;
    }
}
