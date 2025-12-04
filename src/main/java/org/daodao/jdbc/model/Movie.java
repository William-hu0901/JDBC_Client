package org.daodao.jdbc.model;

public class Movie {
    private String title;
    private Integer year;
    private String genre;
    private String description;

    public Movie() {
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Movie(String title, Integer year, String genre, String description) {
        this.title = title;
        this.year = year;
        this.genre = genre;
        this.description = description;
    }
}