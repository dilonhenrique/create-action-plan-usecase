@Injectable()
export class CreateActionPlanUseCase {
  constructor(private hasuraService: HasuraClientService) {}

  private async getDiagnosticById(id: string) {
    const response = await this.hasuraService.query({
      Diagnostics_by_pk: [
        { id },
        {
          id: true,
          status: true,
          customer_id: true,
          analysis: {
            partner_id: true,
          },
          scores: [
            { where: { respondent_id: { _is_null: true } } },
            {
              indicators_scores: [
                {},
                {
                  value: true,
                  indicator_id: true,
                },
              ],
            },
          ],
        },
      ],
    });
    const diagnostic = response?.Diagnostics_by_pk;
    if (!diagnostic) throw new ClientError(ErrorCode.ANALYSIS_NOT_FOUND);
    return diagnostic;
  }

  private async insertActionPlan(payload: {
    diagnostic_id: string;
    partner_id: string;
    customer_id: string;
    name: string;
    start_date: Date;
    end_date: Date;
  }) {
    const response = await this.hasuraService.mutation({
      insert_ActionPlans_one: [
        {
          object: {
            diagnostic_id: payload.diagnostic_id,
            partner_id: payload.partner_id,
            customer_id: payload.customer_id,
            name: payload.name,
            start_date: payload.start_date,
            end_date: payload.end_date,
          },
        },
        {
          id: true,
        },
      ],
    });
    return response.insert_ActionPlans_one.id as string | undefined;
  }

  resolveId(currId: string | undefined, idMap: Map<string, string>) {
    if (currId === undefined) return undefined;

    const mappedId = idMap.get(currId);
    const newId = mappedId ?? randomUUID();
    if (!mappedId) idMap.set(currId, newId);

    return newId;
  }

  regenarateTaskIds(tasks: CreateTaskPayloadDto[]): CreateTaskPayloadDto[] {
    const idMap = new Map<string, string>();

    return tasks.map((task) => {
      const id = this.resolveId(task.id, idMap);
      const original_task_id = this.resolveId(task.original_task_id, idMap);
      const parent_task_id = this.resolveId(task.parent_task_id, idMap);

      return { ...task, id, original_task_id, parent_task_id };
    });
  }

  private async insertTask(payload: TaskPayload) {
    const response = await this.hasuraService.mutation({
      insert_ActionPlanTasks_one: [
        {
          object: {
            id: payload.id,
            action_plan_id: payload.action_plan_id,
            indicator_id: payload.indicator_id,
            name: payload.name,
            description: payload.description,
            credit_value: payload.credit_value,
            start_date: payload.start_date,
            end_date: payload.end_date,
            suggested_task_id: payload.suggested_task_id,
            parent_task_id: payload.parent_task_id,
            original_task_id: payload.original_task_id,
            assignee_type: payload.assignee_type,
            score_increase: payload.score_increase,
          },
        },
        {
          id: true,
        },
      ],
    });
    return response.insert_ActionPlanTasks_one.id as string | undefined;
  }

  private async insertTaskAssignees(
    payload: {
      user_id: string;
      task_id: string;
    }[],
  ) {
    const response = await this.hasuraService.mutation({
      insert_ActionPlanTaskAssigneeAssociation: [
        {
          objects: payload,
        },
        {
          affected_rows: true,
          returning: {
            id: true,
          },
        },
      ],
    });
    return (
      response.insert_ActionPlanTaskAssigneeAssociation.affected_rows ===
      payload.length
    );
  }

  private async insertTag(payload: { name: string; partner_id: string }) {
    const query = await this.hasuraService.query({
      ActionPlanTaskTags: [
        {
          where: {
            name: { _eq: payload.name },
            partner_id: { _eq: payload.partner_id },
          },
        },
        {
          id: true,
        },
      ],
    });

    if (query.ActionPlanTaskTags.length > 0) {
      return query.ActionPlanTaskTags[0].id as string;
    }

    const response = await this.hasuraService.mutation({
      insert_ActionPlanTaskTags_one: [
        {
          object: { name: payload.name, partner_id: payload.partner_id },
        },
        {
          id: true,
        },
      ],
    });
    return response.insert_ActionPlanTaskTags_one?.id as string | undefined;
  }

  private async insertTagAssociations(
    payload: { tag_id: string; task_id: string }[],
  ) {
    const response = await this.hasuraService.mutation({
      insert_ActionPlanTaskTagAssociation: [
        {
          objects: payload,
        },
        {
          affected_rows: true,
          returning: { id: true },
        },
      ],
    });
    return (
      response.insert_ActionPlanTaskTagAssociation.affected_rows ===
      payload.length
    );
  }

  private async getSuggestedTasksByIds(ids: string[]) {
    const response = await this.hasuraService.query({
      ActionServices: [
        { where: { id: { _in: ids } } },
        {
          id: true,
          score_increase_coefficient: true,
        },
      ],
    });
    return response.ActionServices as {
      id: string;
      score_increase_coefficient: number;
    }[];
  }

  private async getTaskScoreIncrease(
    tasks: CreateTaskPayloadDto[],
    indicatorScores: IndicatorScore[],
  ): Promise<(CreateTaskPayloadDto & { score_increase: number })[]> {
    const tasksWithCoef = await Promise.all(
      tasks.map(async (task) => {
        let score_increase_coefficient: number | undefined = undefined;
        if (task.suggested_task_id) {
          const suggestedTask = await this.getSuggestedTasksByIds([
            task.suggested_task_id,
          ]);
          score_increase_coefficient =
            suggestedTask[0]?.score_increase_coefficient;
        }
        return { ...task, score_increase_coefficient };
      }),
    );

    return calculateTaskScoreIncrease(tasksWithCoef, indicatorScores);
  }

  private async transformTasks(
    tasks: CreateTaskPayloadDto[],
    indicatorScores: IndicatorScore[],
  ) {
    const newIdTasks = this.regenarateTaskIds(tasks);
    const taskWithScore = await this.getTaskScoreIncrease(
      newIdTasks,
      indicatorScores,
    );

    return taskWithScore.reduce<ScoreTask[][]>(
      (acc, task) => {
        if (task.parent_task_id) {
          acc[2].push(task);
        } else if (task.original_task_id) {
          acc[1].push(task);
        } else {
          acc[0].push(task);
        }
        return acc;
      },
      [[], [], []],
    );
    // return taskWithScore.sort((a, b) => {
    //   const aOri = Number(!!a.original_task_id);
    //   const aPar = Number(!!a.parent_task_id);
    //   const bOri = Number(!!b.original_task_id);
    //   const bPar = Number(!!b.parent_task_id);
    //   return aOri + aPar - (bOri + bPar);
    // });
  }

  private async handleInsertTasks(
    task: ScoreTask,
    action_plan_id: string,
    tagMap: Map<string, string>,
  ) {
    const task_id = await this.insertTask({
      id: task.id,
      action_plan_id,
      indicator_id: task.indicator_id,
      name: task.name,
      description: task.description,
      credit_value: task.credit_value,
      start_date: task.start_date,
      end_date: task.end_date,
      suggested_task_id: task.suggested_task_id,
      parent_task_id: task.parent_task_id,
      original_task_id: task.original_task_id,
      assignee_type:
        task.assignee_type as unknown as ActionServicesAssigneeType_enum,
      score_increase: task.score_increase,
    });

    if (task.assignee.length > 0) {
      const assigneePayloads = task.assignee.map((a) => ({
        user_id: a,
        task_id,
      }));
      await this.insertTaskAssignees(assigneePayloads);
    }

    if (task.tags.length > 0) {
      const tagPayloads = task.tags.map((tag) => {
        const tag_id = tagMap.get(tag);
        return {
          tag_id,
          task_id,
        };
      });
      await this.insertTagAssociations(tagPayloads);
    }
  }

  async execute(
    id: string,
    payload: CreateActionPlanPayloadDto,
    user: SectionUser,
  ) {
    if (!id) throw new ClientError(ErrorCode.ANALYSIS_NOT_FOUND);

    const diagnostic = await this.getDiagnosticById(id);

    if (diagnostic.analysis.partner_id !== user.partnerId)
      throw new ClientError(ErrorCode.UNAUTHORIZED);

    if (diagnostic.status !== DiagnosticStatus_enum.ANSWERS_DONE)
      throw new ClientError(ErrorCode.ANALYSIS_NOT_READY);

    const action_plan_id = await this.insertActionPlan({
      diagnostic_id: id,
      partner_id: diagnostic.analysis.partner_id as string,
      customer_id: diagnostic.customer_id as string,
      name: payload.name,
      start_date: payload.start_date,
      end_date: payload.end_date,
    });

    const indicatorsScores = diagnostic.scores[0].indicators_scores;
    const [normalTasks, dependentTasks, subTasks] = await this.transformTasks(
      payload.tasks,
      indicatorsScores,
    );

    const tags = payload.tasks.flatMap((task) => task.tags);
    const uniqTags = [...new Set(tags)];
    const tagMap = new Map<string, string>();

    if (uniqTags.length > 0) {
      await Promise.all(
        uniqTags.map(async (tagName) => {
          const tagId = await this.insertTag({
            name: tagName,
            partner_id: user.partnerId,
          });
          tagMap.set(tagName, tagId);
        }),
      );
    }

    normalTasks?.length > 0 &&
      (await Promise.all(
        normalTasks.map(async (task) => {
          await this.handleInsertTasks(task, action_plan_id, tagMap);
        }),
      ));

    dependentTasks?.length > 0 &&
      (await Promise.all(
        dependentTasks.map(async (task) => {
          await this.handleInsertTasks(task, action_plan_id, tagMap);
        }),
      ));

    subTasks?.length > 0 &&
      (await Promise.all(
        subTasks.map(async (task) => {
          await this.handleInsertTasks(task, action_plan_id, tagMap);
        }),
      ));

    return action_plan_id;
  }
}
