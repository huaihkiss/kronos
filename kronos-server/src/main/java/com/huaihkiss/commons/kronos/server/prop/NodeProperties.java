package com.huaihkiss.commons.kronos.server.prop;

import java.io.Serializable;

public class NodeProperties implements Serializable {
    private static final long serialVersionUID = -8867591335451439587L;
    private String id ;
    private boolean isSync;
    private long executeTime;
    private String path;
    public NodeProperties() {
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isSync() {
        return isSync;
    }

    public void setSync(boolean sync) {
        isSync = sync;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
    }
}
