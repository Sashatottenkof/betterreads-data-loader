package com.alex.betterreadsdataloader;

import com.alex.betterreadsdataloader.Author.Author;
import com.alex.betterreadsdataloader.Author.AuthorRepository;
import com.alex.betterreadsdataloader.connection.DataStaxAstraProperties;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {
    @Autowired
    AuthorRepository authorRepository;
    @Value("${datadump.location.authors}")
    private String authorDumpLocation;
    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
    }

    @PostConstruct
    public void start() {
        initAuthors();
    }

    private void initAuthors() {
        Path path = Paths.get(authorDumpLocation);
        try
            (Stream<String> lines = Files.lines(path)){
            lines.limit(10).forEach(line -> {
                // Read and parse the line
                String jasonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jasonString);

                    //Construct Author object
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));

                    //Persist using Repository
                    System.out.println("Saving author: " + author.getName());
                    authorRepository.save(author);

                } catch (JSONException e) {
                    e.printStackTrace();
                }

            });

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * This is necessary to have the Spring Boot app use the Astra secure bundle
     * to connect to the database
     */
    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

}
