import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.ListJacksonDataFormat;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.dataformat.BindyType;

import java.util.Arrays;

public class MyRouteBuilder extends RouteBuilder {
    private final String[] args;
    private final String FROM_URI = "jetty:http://localhost:7000/";
    private final String TO_URI = "file:src/main/resources?fileExist=append&fileName=items-${date:now:dd-MM-yyyy}.csv";
    private final String ROUTE_ID = "Test task";
    private final String HTTP_HEADER_TOKEN_KEY = "Token";
    private final String HTTP_HEADER_TOKEN_VALUE = "SECRET";
    private final String HTTP_HEADER_TOKEN_MISSED_MESSAGE = "Access token is missing or invalid.";
    private final String NUMBER_OF_ITEMS_MESSAGE = "Number of items = ${body.size}";

    public MyRouteBuilder(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) throws Exception {
        CamelContext camelContext = new DefaultCamelContext();
        camelContext.addRoutes(new MyRouteBuilder(args));
        camelContext.start();
    }

    @Override
    public void configure() {
        from(FROM_URI)
            .routeId(ROUTE_ID)
                .choice()
                    .when(exchange -> exchange.getIn().getHeaders().entrySet().stream()
                        .noneMatch(e -> e.getKey().equals(HTTP_HEADER_TOKEN_KEY) && e.getValue().equals(HTTP_HEADER_TOKEN_VALUE)))
                    .log(LoggingLevel.WARN, HTTP_HEADER_TOKEN_MISSED_MESSAGE)
                    .process(exchange -> {
                        exchange.getIn().setBody(HTTP_HEADER_TOKEN_MISSED_MESSAGE);
                        exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 401);
                    })
                    .stop()
                .otherwise()
                    .unmarshal(new ListJacksonDataFormat(Item.class))
                    .log(LoggingLevel.INFO, NUMBER_OF_ITEMS_MESSAGE)
                    .process(exchange -> {
                        if (args.length != 0) {
                            int multiplier = Integer.parseInt(args[0]);
                            Item[] items = exchange.getIn().getBody(Item[].class);
                            Arrays.stream(items).forEach(e -> e.setSum(e.getSum() * multiplier));
                        }
                    })
                    .marshal()
                    .bindy(BindyType.Csv, Item.class)
                    .to(TO_URI);
    }
}
