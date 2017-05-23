package proj.karthik.email.analyzer.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

/**
 * Model class for email header
 */
@JsonRootName(value = "header")
public class Header implements Serializable {
    // Message-ID: <29608191.1075843695188.JavaMail.evans@thyme>
    private String messageId;
    // Date: Thu, 19 Apr 2001 03:45:00 -0700 (PDT)
    private Timestamp timestamp;
    // Subject: Ken Lay's email to Sen. Brulte
    private Subject subject;
//    // Mime-Version
//    private String mimeVersion;
    //Content-Type: text/plain; charset=ANSI_X3.4-1968
    private String contentType;
//    //Content-Transfer-Encoding: 7bit
//    private String contentTransfer;
    //X-From: Jean Munoz <jmunoz@mcnallytemple.com>
    private EmailAddress from;
    //X-To: "'Andy Brown (E-mail)'" <ABB@eslawfirm.com>, <rescalante@riobravo-gm.com>
    private List<EmailAddress> to = new LinkedList<>();
    //X-cc:
    private List<EmailAddress> cc = new LinkedList<>();
    //X-bcc:
    private List<EmailAddress> bcc = new LinkedList<>();
//    //X-Folder: \Jeff_Dasovich_June2001\Notes Folders\Notes inbox
//    private String folder;
//    //X-Origin: DASOVICH-J
//    private String origin;
    //X-FileName: jdasovic.nsf
    private String attachmentName;
    // charset=ANSI_X3.4-1968
    private String charset;

    public Header() {
    }

    @JsonGetter("attachment")
    public void setAttachmentName(final String attachmentName) {
        this.attachmentName = attachmentName;
    }

    @JsonGetter("to")
    public void setTo(final List<EmailAddress> toAddresses) {
        this.to.addAll(toAddresses);
    }

    @JsonGetter("bcc")
    public void setBcc(final List<EmailAddress> bccAddresses) {
        this.bcc.addAll(bccAddresses);
    }

    @JsonGetter("cc")
    public void setCc(final List<EmailAddress> emailAddresses) {
        cc.addAll(emailAddresses);
    }

    @JsonGetter("timestamp")
    public void setTimestamp(final Timestamp dateTime) {
        this.timestamp = dateTime;
    }

    @JsonGetter("from")
    public void setFrom(final EmailAddress emailAddress) {
        this.from = emailAddress;
    }

    @JsonGetter("id")
    public void setId(final String id) {
        this.messageId = id;
    }

    @JsonGetter("subject")
    public void setSubject(final Subject subject) {
        this.subject = subject;
    }

    @JsonGetter("bcc")
    public List<EmailAddress> bcc() {
        return bcc;
    }

    @JsonGetter("attachment")
    public String getAttachmentName() {
        return attachmentName;
    }

    @JsonGetter("cc")
    public List<EmailAddress> cc() {
        return cc;
    }

    @JsonGetter("timestamp")
    public Timestamp timestamp() {
        return timestamp;
    }

    @JsonGetter("from")
    public EmailAddress from() {
        return from;
    }

    @JsonGetter("id")
    public String id() {
        return messageId;
    }

    @JsonGetter("subject")
    public Subject subject() {
        return subject;
    }

    @JsonGetter("to")
    public List<EmailAddress> to() {
        return to;
    }

    @JsonSetter("content_type")
    public void setContentType(final String contentType) {
        this.contentType = contentType;
    }

    @JsonGetter("content_type")
    public String getContentType() {
        return contentType;
    }

    @JsonSetter("charset")
    public void setCharset(final String charset) {
        this.charset = charset;
    }

    @JsonGetter("charset")
    public String getCharset() {
        return charset;
    }
}
