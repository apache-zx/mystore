package cn.unipus.muses.feed.bean;

public class DwdGoodFeature {
    private String label;
    private String good_id;
    private String level;
    private Long feature_count;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getGood_id() {
        return good_id;
    }

    public void setGood_id(String good_id) {
        this.good_id = good_id;
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
        return "DwdGoodFeature{" +
                "label='" + label + '\'' +
                ", good_id='" + good_id + '\'' +
                ", level='" + level + '\'' +
                ", feature_count=" + feature_count +
                '}';
    }
}
