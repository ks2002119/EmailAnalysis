package proj.karthik.email.analyzer.core;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Enumeration of different attributes of an email
 */
public enum EmailAttribute {
    ID("id"),
    DATE("date"),
    SENT_DATE("sent_date"),
    FROM("from"),
    TO("to"),
    SUBJECT("subject"),
    CC("cc"),
    BCC("bcc"),
    FOLDER("folder"),
    ATTACHMENT("attachment"),
    BODY("body"),
    CONTENT_TYPE("content_type");

    private static final Map<String, EmailAttribute> ATTRIBUTE_MAP = new HashMap<>();
    private final String name;
    static {
        Arrays.stream(EmailAttribute.values())
                .forEach(emailAttribute ->
                        ATTRIBUTE_MAP.put(emailAttribute.getName(), emailAttribute));
    }

    EmailAttribute(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Returns the email attribute for the given name.
     * @param name
     * @return emailAttribute
     */
    public static EmailAttribute from(final String name) {
        return ATTRIBUTE_MAP.get(name);
    }
}
