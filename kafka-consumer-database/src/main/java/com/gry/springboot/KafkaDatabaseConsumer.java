package com.gry.springboot;

import com.gry.springboot.entity.WikimediaData;
import com.gry.springboot.repository.WikimediaDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class KafkaDatabaseConsumer {

    private final WikimediaDataRepository dataRepository;

    @KafkaListener(
            topics = "wikimedia_recentchange",
            groupId = "myGroup")
    public void consume(String eventMessage){

        log.info(String.format("Event message consumed -> %s", eventMessage));

        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventData(eventMessage);

        dataRepository.save(wikimediaData);
    }

}
