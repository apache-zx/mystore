package cn.unipus.muses.feed.bean;

public class DwdUserFeature {
    private String label;
    private String user_id;
    private String level;
    private Long feature_count;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public Long getFeature_count() {
        return feature_count;
    }

    public void setFeature_count(Long feature_count) {
        this.feature_count = feature_count;
    }

    @Override
    public String toString() {
        return "DwdUserFeature{" +
                "label='" + label + '\'' +
                ", user_id='" + user_id + '\'' +
                ", level='" + level + '\'' +
                ", feature_count=" + feature_count +
                '}';
    }
}
