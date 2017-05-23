package proj.karthik.email.analyzer.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.io.Serializable;
import java.util.Objects;

/**
 * Model class that represents email address
 */
@JsonRootName(value = "email_address")
public class EmailAddress implements Serializable {
    private String id;
    private String domain;

    public EmailAddress() {
    }

    public EmailAddress(String id, String domain) {
        this.id = id + "@" + domain;
        this.domain = domain;
    }

    @JsonGetter("id")
    public String getId() {
        return id;
    }

    @JsonSetter("id")
    public void setId(final String id) {
        this.id = id;
    }

    @JsonGetter("domain")
    public String getDomain() {
        return domain;
    }

    @JsonSetter("domain")
    public void setDomain(final String domain) {
        this.domain = domain;
    }

    @Override
    public boolean equals(final Object otherEmailAddress) {
        if (this == otherEmailAddress) {
            return true;
        }
        if (otherEmailAddress == null) {
            return false;
        }
        final EmailAddress that = (EmailAddress) otherEmailAddress;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
