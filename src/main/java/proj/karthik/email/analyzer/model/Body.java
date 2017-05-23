package proj.karthik.email.analyzer.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.sql.Timestamp;

/**
 * Created by ksubramanian on 3/1/17.
 */
@JsonRootName("body")
public class Body {

    private Timestamp sentTimestamp;
    private String text;

    @JsonSetter("sent_at")
    public void setEmbeddedTimestamp(final Timestamp sentTimestamp) {
        this.sentTimestamp = sentTimestamp;
    }

    @JsonGetter("sent_at")
    public Timestamp getSentTimestamp() {
        return sentTimestamp;
    }

    @JsonGetter("text")
    public String getText() {
        return text;
    }

    @JsonSetter("text")
    public void setText(String text) {
        this.text = text;
    }
}
