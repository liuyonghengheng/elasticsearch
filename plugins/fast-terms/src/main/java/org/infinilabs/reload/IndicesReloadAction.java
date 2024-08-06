package org.infinilabs.reload;
import org.elasticsearch.action.ActionType;

public class IndicesReloadAction
	extends ActionType<IndicesReloadResponse> {

    public static final IndicesReloadAction INSTANCE =
    		new IndicesReloadAction();
    public static final String NAME =
    		"indices:admin/reload";

    private IndicesReloadAction() {
        super(NAME, IndicesReloadResponse::new);
    }
}
