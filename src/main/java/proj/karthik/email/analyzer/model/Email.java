package proj.karthik.email.analyzer.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * Email model
 */
@JsonRootName("email")
public class Email {

    private Header header;
    private Body body;

    public Email() {
    }

    public Email(final Header header, final Body body) {
        this.header = header;
        this.body = body;
    }

    @JsonGetter("header")
    public Header getHeader() {
        return header;
    }

    @JsonSetter("header")
    public void setHeader(final Header header) {
        this.header = header;
    }

    @JsonGetter("body")
    public Body getBody() {
        return body;
    }

    @JsonSetter("body")
    public void setBody(final Body body) {
        this.body = body;
    }
}
