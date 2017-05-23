package proj.karthik.email.analyzer.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.io.Serializable;

/**
 * Model class that represents subject.
 */
@JsonRootName(value = "subject")
public class Subject implements Serializable {
    private String text;
    private boolean isForward;
    private boolean isReply;

    public Subject(final String text, final boolean isForward, final boolean isReply) {
        this.text = text;
        this.isForward = isForward;
        this.isReply = isReply;
    }

    public Subject() {
    }

    @JsonGetter("text")
    public String getText() {
        return text;
    }

    @JsonGetter("is_forward")
    public boolean isForward() {
        return isForward;
    }

    @JsonGetter("is_reply")
    public boolean isReply() {
        return isReply;
    }

    @JsonSetter("text")
    public void setText(final String text) {
        this.text = text;
    }

    @JsonSetter("is_forward")
    public void setForward(final boolean forward) {
        isForward = forward;
    }

    @JsonSetter("is_reply")
    public void setReply(final boolean reply) {
        isReply = reply;
    }
}
