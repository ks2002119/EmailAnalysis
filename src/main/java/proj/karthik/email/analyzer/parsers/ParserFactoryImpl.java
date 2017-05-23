package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;
import com.google.inject.Injector;

import java.util.EnumMap;

import proj.karthik.email.analyzer.core.AnalyzerException;
import proj.karthik.email.analyzer.core.EmailAttribute;
import proj.karthik.email.analyzer.core.Parser;
import proj.karthik.email.analyzer.core.ParserFactory;

/**
 * Implementation of {@link ParserFactory}
 */
public class ParserFactoryImpl implements ParserFactory {
    private final EnumMap <EmailAttribute, Parser> parserMap =
            new EnumMap<>(EmailAttribute.class);
    private final Injector injector;

    @Inject
    public ParserFactoryImpl(Injector injector) {
        this.injector = injector;
        parserMap.put(EmailAttribute.ATTACHMENT, injector.getInstance(AttachmentParser.class));
        parserMap.put(EmailAttribute.BCC, injector.getInstance(BccAddressParser.class));
        parserMap.put(EmailAttribute.CC, injector.getInstance(CcAddressParser.class));
        parserMap.put(EmailAttribute.DATE, injector.getInstance(DateParser.class));
        parserMap.put(EmailAttribute.FROM, injector.getInstance(FromAddressParser.class));
        parserMap.put(EmailAttribute.ID, injector.getInstance(MessageIdParser.class));
        parserMap.put(EmailAttribute.SUBJECT, injector.getInstance(SubjectParser.class));
        parserMap.put(EmailAttribute.TO, injector.getInstance(ToAddressParser.class));
        parserMap.put(EmailAttribute.CONTENT_TYPE, injector.getInstance(ContentTypeParser.class));
        parserMap.put(EmailAttribute.SENT_DATE, injector.getInstance(SentParser.class));
    }

    @Override
    public Parser getParser(final EmailAttribute emailAttribute) {
        if (emailAttribute != null) {
            if (emailAttribute == EmailAttribute.BODY) {
                // this is to avoid circular dependency in Guice.
                return injector.getInstance(BodyParser.class);
            } else {
                Parser parser = parserMap.get(emailAttribute);
                if (parser == null) {
                    throw new AnalyzerException("Email attribute:%s does not have a valid parser",
                            emailAttribute.getName());
                }
                return parser;
            }
        } else {
            throw new AnalyzerException("Email attribute cannot be null");
        }
    }
}
