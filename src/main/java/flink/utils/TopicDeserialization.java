package flink.utils;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.parseLine;

public class TopicDeserialization {
    private long approveDate;
    private String articleId;
    private long wordCount;
    private long commentId;
    private String commentType;
    private long createDate;
    private int depth;
    private String editorSelection;
    private long inReplyTo;
    private String parentUserDisplayName;
    private int recommendations;
    private String sectionName;
    private String userDisplayName;
    private String userLocation;
    private long userId;
    private String line;

    public TopicDeserialization(String line) {
        parse(line);
    }

    private void parse(String line) {

    }

    public TopicDeserialization(long approveDate, String articleId, long wordCount, long commentId, String commentType, long createDate, int depth, String editorSelection, long inReplyTo, String parentUserDisplayName, int recommendations, String sectionName, String userDisplayName, String userLocation, long userId) {
        this.approveDate = approveDate;
        this.articleId = articleId;
        this.wordCount = wordCount;
        this.commentId = commentId;
        this.commentType = commentType;
        this.createDate = createDate;
        this.depth = depth;
        this.editorSelection = editorSelection;
        this.inReplyTo = inReplyTo;
        this.parentUserDisplayName = parentUserDisplayName;
        this.recommendations = recommendations;
        this.sectionName = sectionName;
        this.userDisplayName = userDisplayName;
        this.userLocation = userLocation;
        this.userId = userId;
    }


}
