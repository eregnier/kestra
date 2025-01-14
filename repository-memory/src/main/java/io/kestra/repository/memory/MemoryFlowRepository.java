package io.kestra.repository.memory;

import io.kestra.core.models.SearchResult;
import io.kestra.core.utils.ListUtils;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.core.value.ValueException;
import io.micronaut.data.model.Pageable;
import io.kestra.core.events.CrudEvent;
import io.kestra.core.events.CrudEventType;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.ArrayListTotal;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.services.FlowService;
import org.apache.commons.lang3.NotImplementedException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.validation.ConstraintViolationException;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
@MemoryRepositoryEnabled
public class MemoryFlowRepository implements FlowRepositoryInterface {
    private final HashMap<String, Flow> flows = new HashMap<>();
    private final HashMap<String, Flow> revisions = new HashMap<>();

    @Inject
    @Named(QueueFactoryInterface.FLOW_NAMED)
    private QueueInterface<Flow> flowQueue;

    @Inject
    @Named(QueueFactoryInterface.TRIGGER_NAMED)
    private QueueInterface<Trigger> triggerQueue;

    @Inject
    private ApplicationEventPublisher eventPublisher;

    @Inject
    private ModelValidator modelValidator;

    private static String flowId(Flow flow) {
        return flowId(flow.getNamespace(), flow.getId());
    }

    private static String flowId(String namespace, String id) {
        return String.join("_", Arrays.asList(
            namespace,
            id
        ));
    }

    @Override
    public Optional<Flow> findById(String namespace, String id, Optional<Integer> revision) {
        return revision
            .map(integer -> this.findRevisions(namespace, id)
                .stream()
                .filter(flow -> flow.getRevision().equals(integer))
                .findFirst()
            )
            .orElseGet(() -> this.flows.containsKey(flowId(namespace, id)) ?
                Optional.of(this.flows.get(flowId(namespace, id))) :
                Optional.empty()
            );
    }

    @Override
    public List<Flow> findRevisions(String namespace, String id) {
        return revisions
            .values()
            .stream()
            .filter(flow -> flow.getNamespace().equals(namespace) && flow.getId().equals(id))
            .sorted(Comparator.comparingInt(Flow::getRevision))
            .collect(Collectors.toList());
    }

    @Override
    public List<Flow> findAll() {
        return new ArrayList<>(flows.values());
    }

    @Override
    public List<Flow> findAllWithRevisions() {
        return new ArrayList<>(revisions.values());
    }

    @Override
    public List<Flow> findByNamespace(String namespace) {
        return flows.values()
            .stream()
            .filter(flow -> flow.getNamespace().equals(namespace))
            .sorted(Comparator.comparingInt(Flow::getRevision))
            .collect(Collectors.toList());
    }

    public ArrayListTotal<Flow> find(String query, Pageable pageable) {
        //TODO Non used query, returns just all at the moment
        if (pageable.getNumber() < 1) {
            throw new ValueException("Page cannot be < 1");
        }

        return ArrayListTotal.of(pageable, this.findAll());
    }

    @Override
    public ArrayListTotal<SearchResult<Flow>> findSourceCode(String query, Pageable pageable) {
        throw new NotImplementedException();
    }

    public Flow create(Flow flow) throws ConstraintViolationException {
        // control if create is valid
        flow.validate()
            .ifPresent(s -> {
                throw s;
            });

        return this.save(flow, CrudEventType.CREATE);
    }

    public Flow update(Flow flow, Flow previous) throws ConstraintViolationException {
        // control if update is valid
        this
            .findById(previous.getNamespace(), previous.getId())
            .map(current -> current.validateUpdate(flow))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .ifPresent(s -> {
                throw s;
            });

        Flow saved = this.save(flow, CrudEventType.UPDATE);

        FlowService
            .findRemovedTrigger(flow, previous)
            .forEach(abstractTrigger -> triggerQueue.delete(Trigger.of(flow, abstractTrigger)));

        return saved;
    }

    private Flow save(Flow flow, CrudEventType crudEventType) throws ConstraintViolationException {
        // validate the flow
        modelValidator
            .isValid(flow)
            .ifPresent(s -> {
                throw s;
            });

        // flow exists, return it
        Optional<Flow> exists = this.findById(flow.getNamespace(), flow.getId());
        if (exists.isPresent() && exists.get().equalsWithoutRevision(flow)) {
            return exists.get();
        }

        List<Flow> revisions = this.findRevisions(flow.getNamespace(), flow.getId());

        if (revisions.size() > 0) {
            flow = flow.withRevision(revisions.get(revisions.size() - 1).getRevision() + 1);
        } else {
            flow = flow.withRevision(1);
        }

        this.flows.put(flowId(flow), flow);
        this.revisions.put(flow.uid(), flow);

        flowQueue.emit(flow);
        eventPublisher.publishEvent(new CrudEvent<>(flow, crudEventType));

        return flow;
    }

    @Override
    public Flow delete(Flow flow) {
        if (this.findById(flow.getNamespace(), flow.getId(), Optional.of(flow.getRevision())).isEmpty()) {
            throw new IllegalStateException("Flow " + flow.getId() + " doesn't exists");
        }

        Flow deleted = flow.toDeleted();

        flowQueue.emit(deleted);
        this.flows.remove(flowId(deleted));
        this.revisions.put(deleted.uid(), deleted);

        ListUtils.emptyOnNull(flow.getTriggers())
            .forEach(abstractTrigger -> triggerQueue.delete(Trigger.of(flow, abstractTrigger)));

        eventPublisher.publishEvent(new CrudEvent<>(flow, CrudEventType.DELETE));

        return deleted;
    }

    @Override
    public List<String> findDistinctNamespace() {
        HashSet<String> namespaces = new HashSet<>();
        for (Flow f : this.findAll()) {
            namespaces.add(f.getNamespace());
        }

        ArrayList<String> namespacesList = new ArrayList<>(namespaces);
        Collections.sort(namespacesList);
        return new ArrayList<>(namespacesList);
    }
}
