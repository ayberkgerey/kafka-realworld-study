package com.gry.springboot.entity;


import jakarta.persistence.*;
import lombok.Data;

@Entity
@Data
@Table(name = "wikimedia_recentchange")
public class WikimediaData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Lob
    private String wikiEventData;
}
