package proj.karthik.email.analyzer.report.spark.beans;

import java.io.Serializable;

/**
 * Created by ksubramanian on 3/3/17.
 */
public class DirectCount implements Serializable {
    private String emailId;
    private long directCount;

    public DirectCount() {
    }

    public DirectCount(final String emailId, final long directCount) {
        this.emailId = emailId;
        this.directCount = directCount;
    }

    public String getEmailId() {
        return emailId;
    }

    public void setEmailId(final String emailId) {
        this.emailId = emailId;
    }

    public long getDirectCount() {
        return directCount;
    }

    public void setDirectCount(final long directCount) {
        this.directCount = directCount;
    }
}
