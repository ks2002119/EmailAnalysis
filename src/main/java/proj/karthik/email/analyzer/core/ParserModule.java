package proj.karthik.email.analyzer.core;

import com.google.inject.AbstractModule;

import proj.karthik.email.analyzer.parsers.AttachmentParser;
import proj.karthik.email.analyzer.parsers.BccAddressParser;
import proj.karthik.email.analyzer.parsers.BodyParser;
import proj.karthik.email.analyzer.parsers.CcAddressParser;
import proj.karthik.email.analyzer.parsers.ContentTypeParser;
import proj.karthik.email.analyzer.parsers.DateParser;
import proj.karthik.email.analyzer.parsers.FromAddressParser;
import proj.karthik.email.analyzer.parsers.MessageIdParser;
import proj.karthik.email.analyzer.parsers.ParserFactoryImpl;
import proj.karthik.email.analyzer.parsers.SubjectParser;
import proj.karthik.email.analyzer.parsers.ToAddressParser;

/**
 * Bindings related to parsers.
 */
public class ParserModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ParserFactory.class).to(ParserFactoryImpl.class);
        bind(AttachmentParser.class).asEagerSingleton();
        bind(BccAddressParser.class).asEagerSingleton();
        bind(CcAddressParser.class).asEagerSingleton();
        bind(DateParser.class).asEagerSingleton();
        bind(FromAddressParser.class).asEagerSingleton();
        bind(MessageIdParser.class).asEagerSingleton();
        bind(SubjectParser.class).asEagerSingleton();
        bind(ToAddressParser.class).asEagerSingleton();
        bind(BodyParser.class).asEagerSingleton();
        bind(ContentTypeParser.class).asEagerSingleton();
    }
}
