package com.alex.betterreadsdataloader;

import com.alex.betterreadsdataloader.Author.Author;
import com.alex.betterreadsdataloader.Author.AuthorRepository;
import com.alex.betterreadsdataloader.Book.Book;
import com.alex.betterreadsdataloader.Book.BookRepository;
import com.alex.betterreadsdataloader.connection.DataStaxAstraProperties;
import org.json.JSONArray;
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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {
    @Autowired
    AuthorRepository authorRepository;
    @Autowired
    BookRepository bookRepository;
    @Value("${datadump.location.authors}")
    private String authorDumpLocation;
    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
    }

    @PostConstruct
    public void start() {
//        initAuthors();
        initWorks();
    }

    private void initAuthors() {
        Path path = Paths.get(authorDumpLocation);
        try
                (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
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


    public void initWorks() {
        Path path = Paths.get(worksDumpLocation);
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try
                (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
                // Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    //Construct book object
                    Book book = new Book();
                    book.setId(jsonObject.getString("key").replace("/works/", ""));
                    book.setTitle(jsonObject.optString("title"));
                    JSONObject descriptionObject = jsonObject.optJSONObject("description");
                    if (descriptionObject != null) {
                        book.setDescription(descriptionObject.optString("value"));
                    }
                    JSONArray authorsJsonArray = jsonObject.optJSONArray("authors");

                    if (authorsJsonArray != null) {
                        List<String> authorIds = new ArrayList<>();
                        for(int i=0; i<authorsJsonArray.length(); i++){
                            String authorId = authorsJsonArray.getJSONObject(i).getJSONObject("author").getString("key")
                                    .replace("/authors/","");
                            authorIds.add(authorId);
                        }
                        book.setAuthorId(authorIds);
                        List<String>authorNames= authorIds.stream().map(id ->authorRepository.findById(id))
                                        .map(optionalAuthor->{
                                            if(!optionalAuthor.isPresent()) return "Unknown author";
                                        return optionalAuthor.get().getName();
                                        }).collect(Collectors.toList());
                        book.setAuthorNames(authorNames);
                    }
                    JSONArray coversJsonArray = jsonObject.optJSONArray("covers");
                    if (coversJsonArray != null) {
                        List<String> coversId = new ArrayList<>();
                        for (int i = 0; i < coversJsonArray.length(); i++) {
                            coversId.add(coversJsonArray.getString(i));
                        }
                        book.setCoverIds(coversId);
                    }
                    JSONObject publishedObject = jsonObject.optJSONObject("created");
                    if (publishedObject != null) {
                        String dateStr = publishedObject.getString("value");
                        book.setPublishDate(LocalDate.parse(dateStr, dateFormat));

                    }

                    //Persist using Repository
                    System.out.println("Saving book: " + book.getTitle());
                    bookRepository.save(book);


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
